use lotse_raft::Role::*;

use super::{Cause, Seen, reason};

fn seen(role: lotse_raft::Role, term: u64, leader: Option<u64>) -> Seen {
    Seen { role, term, leader }
}

#[test]
fn the_reason_follows_from_what_changed_and_what_ran() {
    let cases = [
        (seen(Leader, 3, Some(1)), seen(Follower, 3, None), Cause::Tick, "check-quorum"),
        (seen(Follower, 3, Some(2)), seen(PreCandidate, 3, None), Cause::Tick, "pre-votes"),
        (seen(Follower, 3, None), seen(Candidate, 4, None), Cause::Tick, "election timeout: campaigning"),
        (seen(PreCandidate, 3, None), seen(PreCandidate, 3, None), Cause::Tick, "asking again"),
        (seen(PreCandidate, 3, None), seen(Candidate, 4, None), Cause::Message(2), "pre-vote won"),
        (seen(Candidate, 4, None), seen(Leader, 4, Some(1)), Cause::Message(3), "election won"),
        (seen(Leader, 4, Some(1)), seen(Follower, 5, None), Cause::Message(2), "a higher term"),
        (seen(Leader, 4, Some(1)), seen(Follower, 4, Some(2)), Cause::Message(2), "handed over"),
        (seen(Candidate, 4, None), seen(Follower, 4, Some(2)), Cause::Message(2), "a leader was heard"),
        (seen(Candidate, 4, None), seen(Follower, 5, None), Cause::Message(2), "back to following"),
        (seen(Follower, 4, Some(1)), seen(Candidate, 5, None), Cause::Message(1), "asked to campaign"),
        (seen(Follower, 4, Some(1)), seen(Follower, 5, None), Cause::Message(2), "a higher term"),
        (seen(Follower, 4, None), seen(Follower, 4, Some(2)), Cause::Message(2), "a leader was heard"),
    ];
    for (before, after, cause, expected) in cases {
        let r = reason(before, after, cause);
        assert!(r.contains(expected), "{before:?} -> {after:?} after {cause:?}: {r}");
    }
}

#[test]
fn a_change_is_logged_once() {
    let mut e = super::Elections::default();
    e.observe(1, 7, seen(Follower, 1, None), Cause::Tick);
    assert_eq!(e.seen.get(&7), Some(&seen(Follower, 1, None)));
    e.observe(1, 7, seen(Leader, 2, Some(1)), Cause::Message(2));
    e.observe(1, 7, seen(Leader, 2, Some(1)), Cause::Tick);
    assert_eq!(e.seen.get(&7), Some(&seen(Leader, 2, Some(1))));
    e.retain(|g| g != 7);
    assert!(e.seen.is_empty());
}
