//! Waiting for several futures at once, deterministically.
//!
//! A node's loop persists the groups of a round together (ADR 0143): each
//! group's syncs are under way at the same time as the others', so a round
//! costs about the slowest group's syncs, not their sum. The futures are
//! polled in the order given, every time any of them may progress, which
//! keeps a simulation's runs repeatable.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;

/// Runs every future to its end, all of them at once, and returns their
/// outputs in the order the futures were given.
pub(crate) async fn all<F: Future>(futures: impl IntoIterator<Item = F>) -> Vec<F::Output> {
    let mut pending: Vec<Option<Pin<Box<F>>>> = futures.into_iter().map(|f| Some(Box::pin(f))).collect();
    let mut outputs: Vec<Option<F::Output>> = pending.iter().map(|_| None).collect();
    poll_fn(|cx| {
        let mut done = true;
        for (slot, out) in pending.iter_mut().zip(outputs.iter_mut()) {
            if let Some(f) = slot {
                match f.as_mut().poll(cx) {
                    Poll::Ready(value) => {
                        *out = Some(value);
                        *slot = None;
                    }
                    Poll::Pending => done = false,
                }
            }
        }
        if done { Poll::Ready(()) } else { Poll::Pending }
    })
    .await;
    outputs.into_iter().map(|o| o.expect("every future finished")).collect()
}

#[cfg(test)]
mod tests;
