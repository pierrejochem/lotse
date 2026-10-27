//! A runtime-neutral wake-up: a flag and the tasks waiting for it.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

#[derive(Default)]
struct State {
    set: bool,
    waiters: Vec<Waker>,
}

/// Set once by anyone, seen by every waiter. [`Signal::take`] clears it
/// again, for signals that mean "there is new work".
#[derive(Clone, Default)]
pub(crate) struct Signal(Arc<Mutex<State>>);

impl std::fmt::Debug for Signal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Signal").field("set", &self.is_set()).finish()
    }
}

impl Signal {
    pub(crate) fn set(&self) {
        let waiters = {
            let mut s = self.0.lock().expect("signal lock");
            s.set = true;
            std::mem::take(&mut s.waiters)
        };
        waiters.into_iter().for_each(Waker::wake);
    }

    pub(crate) fn is_set(&self) -> bool {
        self.0.lock().expect("signal lock").set
    }

    /// Clears the signal; returns whether it was set.
    pub(crate) fn take(&self) -> bool {
        std::mem::take(&mut self.0.lock().expect("signal lock").set)
    }

    fn poll_set(&self, waker: &Waker) -> bool {
        let mut s = self.0.lock().expect("signal lock");
        if s.set {
            return true;
        }
        if !s.waiters.iter().any(|w| w.will_wake(waker)) {
            s.waiters.push(waker.clone());
        }
        false
    }

    /// Completes once the signal is set.
    pub(crate) async fn wait(&self) {
        poll_fn(|cx| if self.poll_set(cx.waker()) { Poll::Ready(()) } else { Poll::Pending }).await
    }
}

/// Runs `fut` until it completes or one of `signals` is set; `None` in the
/// second case.
pub(crate) async fn race<F: Future>(fut: F, signals: &[&Signal]) -> Option<F::Output> {
    let mut fut = pin!(fut);
    poll_fn(|cx| {
        if signals.iter().any(|s| s.poll_set(cx.waker())) {
            return Poll::Ready(None);
        }
        fut.as_mut().poll(cx).map(Some)
    })
    .await
}
