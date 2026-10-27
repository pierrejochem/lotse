//! A single-threaded, seeded executor with virtual time.
//!
//! Each step picks one runnable task at random (from the seed) and polls it.
//! When nothing is runnable, the clock jumps to the earliest timer. Real time
//! never passes, so an hour of simulated timeouts runs in microseconds.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use lotse_rt::{Instant, Rng};

use crate::SimRng;

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Steps after which a run that makes no progress counts as hung.
const MAX_STEPS: u64 = 50_000_000;

struct State {
    now: Instant,
    next_id: u64,
    tasks: BTreeMap<u64, Option<Task>>,
    runnable: BTreeSet<u64>,
    timers: BTreeMap<(Instant, u64), Waker>,
    timer_seq: u64,
}

#[derive(Clone)]
pub(crate) struct Executor {
    state: Arc<Mutex<State>>,
    rng: SimRng,
}

impl fmt::Debug for Executor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.state.lock().expect("executor lock");
        f.debug_struct("Executor").field("now", &s.now).field("tasks", &s.tasks.len()).finish()
    }
}

struct TaskWaker {
    id: u64,
    state: Arc<Mutex<State>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let mut s = self.state.lock().expect("executor lock");
        if s.tasks.contains_key(&self.id) {
            s.runnable.insert(self.id);
        }
    }
}

impl Executor {
    pub(crate) fn new(rng: SimRng) -> Self {
        Executor {
            state: Arc::new(Mutex::new(State {
                now: Instant::ZERO,
                next_id: 0,
                tasks: BTreeMap::new(),
                runnable: BTreeSet::new(),
                timers: BTreeMap::new(),
                timer_seq: 0,
            })),
            rng,
        }
    }

    pub(crate) fn now(&self) -> Instant {
        self.state.lock().expect("executor lock").now
    }

    pub(crate) fn sleep(&self, duration: Duration) -> Sleep {
        Sleep { state: self.state.clone(), deadline: self.now() + duration, registered: false }
    }

    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let slot = Arc::new(Mutex::new(Slot { value: None, waker: None }));
        let out = slot.clone();
        let task: Task = Box::pin(async move {
            let value = future.await;
            let mut s = out.lock().expect("join slot");
            s.value = Some(value);
            if let Some(w) = s.waker.take() {
                w.wake();
            }
        });
        let mut s = self.state.lock().expect("executor lock");
        let id = s.next_id;
        s.next_id += 1;
        s.tasks.insert(id, Some(task));
        s.runnable.insert(id);
        JoinHandle { slot }
    }

    pub(crate) fn block_on<F, T>(&self, future: F, seed: u64) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = self.spawn(future);
        let mut steps = 0u64;
        loop {
            if let Some(v) = handle.slot.lock().expect("join slot").value.take() {
                return v;
            }
            steps += 1;
            assert!(steps < MAX_STEPS, "simulation with seed {seed} made no progress after {MAX_STEPS} steps");
            let next = {
                let mut s = self.state.lock().expect("executor lock");
                if s.runnable.is_empty() {
                    // Nothing can run: jump to the earliest timer.
                    let Some((&(deadline, seq), _)) = s.timers.iter().next() else {
                        panic!("simulation with seed {seed} deadlocked: no runnable task and no timer");
                    };
                    let waker = s.timers.remove(&(deadline, seq)).expect("present");
                    s.now = s.now.max(deadline);
                    drop(s);
                    waker.wake();
                    continue;
                }
                let n = s.runnable.len() as u64;
                let pick = self.rng.below(n) as usize;
                let id = *s.runnable.iter().nth(pick).expect("in range");
                s.runnable.remove(&id);
                let task = s.tasks.get_mut(&id).and_then(Option::take);
                task.map(|t| (id, t))
            };
            let Some((id, mut task)) = next else { continue };
            let waker = Waker::from(Arc::new(TaskWaker { id, state: self.state.clone() }));
            let done = task.as_mut().poll(&mut Context::from_waker(&waker)).is_ready();
            let mut s = self.state.lock().expect("executor lock");
            if done {
                s.tasks.remove(&id);
                s.runnable.remove(&id);
            } else if let Some(slot) = s.tasks.get_mut(&id) {
                *slot = Some(task);
            }
        }
    }
}

pub(crate) struct Sleep {
    state: Arc<Mutex<State>>,
    deadline: Instant,
    registered: bool,
}

impl Future for Sleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut s = self.state.lock().expect("executor lock");
        if s.now >= self.deadline {
            return Poll::Ready(());
        }
        if !self.registered {
            let seq = s.timer_seq;
            s.timer_seq += 1;
            s.timers.insert((self.deadline, seq), cx.waker().clone());
            drop(s);
            self.registered = true;
        }
        Poll::Pending
    }
}

struct Slot<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

/// Waits for a task spawned on the simulation.
pub struct JoinHandle<T> {
    slot: Arc<Mutex<Slot<T>>>,
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JoinHandle")
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut s = self.slot.lock().expect("join slot");
        match s.value.take() {
            Some(v) => Poll::Ready(v),
            None => {
                s.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
