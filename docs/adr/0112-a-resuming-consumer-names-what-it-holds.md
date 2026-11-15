# 112. A resuming consumer names what it holds

- Status: Accepted (refines [ADR 0109](0109-deliveries-are-written-to-the-log.md))
- Date: 2026-11-15

## Context

Since ADR 0109 a queue leader writes a CHECKOUT batch before it pushes a delivery, and a new leader keeps every message checked out to a consumer until that consumer resumes with its id. When it resumes, the new subscription takes all of them back.

A checkout can commit without its push reaching the consumer: the leader goes, or steps down, or the connection breaks after the batch is written and before the push arrives. The resumed subscription then holds a message the consumer never saw. Nothing gives it back until the ack timeout (30 minutes by default). Three tests against real nodes failed intermittently this way after ADR 0109: the Java test that kills a leader during consumption, and two Node.js tests (a purge that counted one message short, a closed consumer whose messages did not come back).

A second case has the same shape. A consumer that closes just after its leader moved sends its `Unsubscribe` and its give-back `Nack` to the old leader, which refuses them. The new leader keeps the consumer's messages for the resume window (30 seconds), waiting for a consumer that has closed.

Three ways were weighed: the resuming consumer names what it holds; the leader's answer names what it took back and the client nacks what it lacks; or a shorter resume window.

## Decision

**A resuming subscription names what it holds.** `Subscribe` gains tag 3 `held`: ranges `[from, to)`, sent with `resume`. The client names the messages handed out and not yet settled (acknowledged, nacked, or acknowledged in a transaction), those received and not handed out, and those whose requeue waits to be sent. The leader takes back only the named messages of what waits for the consumer. It holds the consumer's other messages back like ones whose resume window is over, and hands them out again once their count is written: at once, not after the ack timeout. Without the tag the leader takes back everything, as before, so an older client works unchanged.

A subscription that names what it holds is kept until it has resumed, also if it unsubscribes first. A leader that has not yet adopted the log's checkouts therefore still sees it when it adopts, and gives back what it did not name.

**Closing gives back at the new leader.** When a consumer with an id closes and its `Unsubscribe` is not answered `NONE`, or its subscription had already ended for a failover or a broken connection, it subscribes once more. It sends `resume`, no credit and `held` naming only the messages it handed out, then unsubscribes. The leader hands the rest out again at once. This costs two requests, and only in that case. The Rust client does it in `close`, not when a consumer is dropped, since dropping cannot wait for a new leader. The Java, C# and Node.js clients do it when they close.

Rejected options:

- **The answer names what was taken back, and the client nacks the rest.** It needs no new request field. But it costs a round trip, and the nack can cross deliveries already on their way to the new subscription.
- **A shorter resume window.** It is only a time limit: a lost push would come back after the window rather than the ack timeout, and transactional acks would have less time to resume.

## Consequences

- A message whose push was lost in a failover goes out again as soon as its consumer resumes. A consumer that closes after a failover leaves nothing waiting for it.
- Clients keep the offsets they handed out until those are settled. The list is as long as what the application holds unacknowledged.
- The protocol gains one tag, so the golden vectors gain two `Subscribe` samples. An older node ignores the tag, as it ignores every unknown tag.
- Tests:
  - Dispatcher unit tests check that a resuming subscription takes back only what it names, that the rest is held back for its count at once, and that a subscription that names nothing and unsubscribes before adoption gives everything back.
  - The queue simulation `a_message_whose_push_was_lost_in_a_failover_comes_again_at_once` kills the leader around each of twelve deliveries, with an ack timeout and a resume window of ten minutes, and requires every message acknowledged within a minute.
  - The queue simulation `a_consumer_that_closes_after_a_failover_gives_back_what_it_did_not_hand_out` requires the messages a consumer received and did not hand out to reach another consumer at once.
  - Each SDK has unit tests for the held list and for closing.
- Mutation-checked:
  - a leader that ignores the list fails the lost-push simulation and a dispatcher unit test;
  - a client that leaves out the messages it handed out fails `a_transactional_ack_survives_a_failover`;
  - a `close` that does not give back fails the closing simulation;
  - dropping a closing subscription before it resumed fails a dispatcher unit test;
  - in the Node.js SDK, not sending the list, not tracking handed-out messages, not forgetting settled ones, and never or always giving back on close each fail a unit test. The Java and C# SDKs have the same tests.
