# 106. A requeue names its subscription

- Status: Accepted
- Date: 2026-11-12

## Context

A requeueing `Nack` goes on the subscription's own connection, and the node takes back the named offsets that this **connection** holds in flight, across every subscription on it. Clients send such a `Nack` when the application nacks a message, and when a consumer ends: after `Unsubscribe` they give back what was pushed to it but not handed out (ADR 0031).

The queue simulation found where that is too wide (seed 2196):
1. A consumer on a single-active-consumer queue was dropped while messages 118 and 121 were pushed to it but unread. The client sends its cleanup `Nack` in the background.
2. The leader's term changed. That ended the subscription, and the messages went back.
3. A newer subscription of the same client, on the same connection, received them again, then 125.
4. The old consumer's `Nack` arrived. It took 118 and 121 back from the newer subscription, which then received 121 a second time, after 125.

The queue's order across one subscription broke. Every such needless redelivery also raises the message's delivery count, so `max.deliveries` (ADR 0055) can dead-letter it early.

## Decision

**A requeueing `Nack` names the subscription its messages came on, and the node takes back only what that subscription holds** (maintainer's choice).

- `Nack` gets request tag 0, `subscription_id` (one `u32`). With it, the node requeues the named offsets in flight with that subscription on this connection, and nothing else. Without it, every subscription of the connection counts, as before.
- The Rust client, the Java SDK (and so the Kotlin layer), the C# SDK and the Node.js SDK send the tag on every requeue:
  - when a consumer ends, the subscription it ends;
  - for `nack` and `nack_after`, the subscription the message came on.

  A discarding `Nack` takes nothing back and names none.
- A message the node already gave back when its subscription ended is not taken from a newer subscription that holds it again. The newer one keeps it, and the late `Nack` changes nothing.

## Consequences

- A late requeue from a consumer that ended no longer reorders a single active consumer's messages, and no longer raises delivery counts of messages a newer subscription holds.
- Older clients, without the tag, keep the connection-wide behaviour, and with it the race.
- Tests:
  - A server unit test: a late `Nack` naming an ended subscription leaves the message with the newer one; a `Nack` naming the holder gives it back; one without a name still takes it from any subscription of the connection.
  - The queue simulation at seed 2196.
  - Each client checks the tag on what it sends, and the codec and vectors carry a tagged `Nack`.
  - Mutation checks:
    - Ignoring the tag on the node fails the unit test and the seed.
    - A Rust client that does not name the subscription when a consumer ends fails the seed.
    - Java and Node.js consumers that leave out the tag on close or on nack fail their tests.
    - C# runs only in CI.
