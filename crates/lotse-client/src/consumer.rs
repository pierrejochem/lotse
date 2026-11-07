//! Consuming a queue: [`Client::subscribe`] and [`Consumer`].
//!
//! A subscription lives on one connection to the queue's leader. The leader
//! pushes batches as stored with the offsets in them that are this
//! subscription's, never more messages than the consumer has credit for.
//! The consumer hands them out one at a time and adds credit as they are
//! used up, so at most `credit` messages wait unprocessed on this side.
//!
//! When the subscription ends (the leader changed, the connection broke, or
//! the consumer held a message longer than the queue's ack timeout) the
//! leader gives its unacknowledged messages to other consumers, so messages
//! waiting here are dropped, and the consumer subscribes again on the next
//! call. Acknowledging a message that went to someone else meanwhile still
//! settles it: an ack names an offset, not a delivery.

use std::collections::{BTreeSet, VecDeque};
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use lotse_proto::{
    AckRequest, CreditRequest, ErrorCode, NackRequest, Request, Response, SubscribeRequest, UnsubscribeRequest,
};
use lotse_rt::{Clock, Net, Rng, Spawn};

use crate::{Attempt, CallError, Client, ClientError, Connection, PushQueue, Transaction, race};

/// One message from a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The record; its offset identifies the message for acks.
    pub record: lotse_record::Record,
    /// How often the message has been delivered, this time included. More
    /// than 1 means an earlier delivery was not acknowledged.
    pub delivery_count: u16,
}

impl Message {
    /// The message's offset in the queue.
    pub fn offset(&self) -> u64 {
        self.record.offset
    }
}

/// The requests that end a subscription, and its connection.
type Cleanup<C> = (Arc<Connection<C>>, Vec<Request>);

struct Sub<C: lotse_rt::Conn> {
    conn: Arc<Connection<C>>,
    id: u32,
    pushes: PushQueue,
}

/// A subscription to a queue, from [`Client::subscribe`]. Read messages with
/// [`Consumer::next`] and settle each with [`Consumer::ack`] or
/// [`Consumer::nack`].
pub struct Consumer<'a, R: Net + Clock + Spawn + Rng> {
    client: &'a Client<R>,
    queue: String,
    resource_id: u64,
    credit: u32,
    sub: Option<Sub<R::Conn>>,
    /// Delivered to this subscription, not handed out yet.
    buffered: VecDeque<Message>,
    /// Messages and bytes used since credit was last added.
    used: (u32, u32),
}

impl<R: Net + Clock + Spawn + Rng> std::fmt::Debug for Consumer<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("queue", &self.queue)
            .field("subscription", &self.sub.as_ref().map(|s| s.id))
            .field("buffered", &self.buffered.len())
            .finish()
    }
}

impl<R: Net + Clock + Spawn + Rng> Client<R> {
    /// Subscribes to a queue with room for `credit` messages at a time
    /// (at least 1).
    pub async fn subscribe(&self, queue: &str, credit: u32) -> Result<Consumer<'_, R>, ClientError> {
        let mut c = Consumer {
            client: self,
            queue: queue.into(),
            resource_id: 0,
            credit: credit.max(1),
            sub: None,
            buffered: VecDeque::new(),
            used: (0, 0),
        };
        c.ensure_subscribed().await?;
        Ok(c)
    }

    /// Drops a connection that failed a call.
    fn drop_conn(&self, addr: &str, conn: &Arc<Connection<R::Conn>>) {
        let mut conns = self.conns.lock().expect("conns lock");
        if conns.get(addr).is_some_and(|c| Arc::ptr_eq(c, conn)) {
            conns.remove(addr);
        }
        conn.close();
    }
}

impl<R: Net + Clock + Spawn + Rng> Consumer<'_, R> {
    async fn ensure_subscribed(&mut self) -> Result<(), ClientError> {
        if self.sub.is_none() {
            let (sub, resource_id) = self.client.retrying(|| self.subscribe_once()).await?;
            self.sub = Some(sub);
            self.resource_id = resource_id;
            self.used = (0, 0);
        }
        Ok(())
    }

    async fn subscribe_once(&self) -> Result<(Sub<R::Conn>, u64), Attempt> {
        let c = self.client;
        let (route, addr) = c.route(&self.queue, 0).await?;
        let conn = c.connection(&addr).await.map_err(|e| Attempt::Retry(e.to_string()))?;
        let req = Request::Subscribe(SubscribeRequest {
            queue_id: route.resource_id,
            consumer_tag: String::new(),
            credit_msgs: self.credit,
            credit_bytes: u32::MAX,
            exclusive: false,
        });
        let resp = match conn.call(&req, &c.rt, c.cfg.request_timeout).await {
            Ok(Response::Subscribe(r)) => r,
            Ok(other) => return Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            Err(e) => {
                // A subscription the broker may have made ends with the
                // connection.
                c.drop_conn(&addr, &conn);
                c.forget(&self.queue, 0);
                return Err(e.into());
            }
        };
        match resp.error {
            ErrorCode::NONE => match conn.register(resp.subscription_id) {
                Some(pushes) => Ok((Sub { conn, id: resp.subscription_id, pushes }, route.resource_id)),
                None => Err(Attempt::Retry("the connection broke".into())),
            },
            code if code.retriable() => {
                c.redirect(&self.queue, 0, resp.leader_hint);
                Err(Attempt::Retry(code.to_string()))
            }
            code => {
                // Deleted: the name may come back as a new queue.
                if code == ErrorCode::UNKNOWN_RESOURCE {
                    c.forget(&self.queue, 0);
                }
                Err(Attempt::Fail(ClientError::Broker { code, message: None }))
            }
        }
    }

    /// The next message, waiting up to `wait` for one; `None` if none came.
    /// Subscribes again first if the subscription has ended.
    pub async fn next(&mut self, wait: Duration) -> Result<Option<Message>, ClientError> {
        let deadline = self.client.rt.now() + wait;
        loop {
            if let Some(m) = self.buffered.pop_front() {
                self.used.0 += 1;
                self.refill().await;
                return Ok(Some(m));
            }
            self.ensure_subscribed().await?;
            let now = self.client.rt.now();
            if now >= deadline {
                return Ok(None);
            }
            let pushes = self.sub.as_ref().expect("subscribed").pushes.clone();
            let next = poll_fn(|cx| {
                let mut q = pushes.lock().expect("push lock");
                match q.0.pop_front() {
                    Some(p) => Poll::Ready(p),
                    None => {
                        q.1 = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            });
            match race(next, self.client.rt.sleep(deadline - now)).await {
                None => return Ok(None),
                Some(Err(_broken)) => self.ended(),
                Some(Ok(push)) if push.error != ErrorCode::NONE => {
                    if push.error == ErrorCode::NOT_LEADER {
                        self.client.redirect(&self.queue, 0, push.leader_hint);
                    } else if push.error == ErrorCode::UNKNOWN_RESOURCE {
                        self.client.forget(&self.queue, 0);
                    }
                    self.ended();
                }
                Some(Ok(push)) => {
                    let corrupt = |e: lotse_record::Error| ClientError::Broker {
                        code: ErrorCode::CORRUPT_BATCH,
                        message: Some(e.to_string()),
                    };
                    let (batch, _) = lotse_record::Batch::parse(&push.batch).map_err(corrupt)?;
                    let mine: std::collections::BTreeMap<u64, u16> =
                        push.deliveries.iter().map(|d| (d.offset, d.delivery_count)).collect();
                    for record in batch.records().map_err(corrupt)? {
                        if let Some(&delivery_count) = mine.get(&record.offset) {
                            self.buffered.push_back(Message { record, delivery_count });
                        }
                    }
                    self.used.1 = self.used.1.saturating_add(push.batch.len() as u32);
                }
            }
        }
    }

    /// The subscription is over: its messages go to others, so the ones
    /// waiting here are dropped.
    fn ended(&mut self) {
        if let Some(sub) = self.sub.take() {
            sub.conn.unregister(sub.id);
        }
        self.buffered.clear();
    }

    /// Adds credit once half of it is used up.
    async fn refill(&mut self) {
        if self.used.0 < self.credit.div_ceil(2) {
            return;
        }
        let Some(sub) = &self.sub else { return };
        let req =
            Request::Credit(CreditRequest { subscription_id: sub.id, add_msgs: self.used.0, add_bytes: self.used.1 });
        // A lost credit is noticed as a broken connection by `next`.
        let _ = sub.conn.send_only(&req).await;
        self.used = (0, 0);
    }

    /// Acknowledges a message: once this returns, it is never delivered
    /// again.
    pub async fn ack(&self, message: &Message) -> Result<(), ClientError> {
        let offset = message.offset();
        // Settling twice is harmless, so an unknown outcome is retried.
        self.client.retrying(|| self.settle_once(offset, None)).await
    }

    /// Acknowledges a message within a transaction (ADR 0075): it is settled
    /// if the transaction commits and delivered again if it aborts, and
    /// until then no one else receives it. Adds the queue to the
    /// transaction first. Only a message this subscription holds can be
    /// acknowledged so; any error leaves the transaction to abort.
    pub async fn ack_in(&self, txn: &mut Transaction<'_, R>, message: &Message) -> Result<(), ClientError> {
        txn.add(&self.queue, 0).await?;
        let res = self.ack_in_once(txn, message.offset()).await;
        if let Err(e) = &res {
            txn.poisoned = Some(e.to_string());
        }
        res
    }

    async fn ack_in_once(&self, txn: &Transaction<'_, R>, offset: u64) -> Result<(), ClientError> {
        let gone = |why: &str| ClientError::Broker { code: ErrorCode::MESSAGE_NOT_HELD, message: Some(why.into()) };
        // The messages are the subscription's connection's: on another one
        // they are not held.
        let Some(sub) = &self.sub else { return Err(gone("the subscription ended; the message went back")) };
        let c = self.client;
        let req = Request::Ack(AckRequest {
            queue_id: self.resource_id,
            txn_id: Some(txn.txn_id.clone()),
            tenant: c.cfg.tenant.clone(),
            producer: Some((txn.producer_id, txn.epoch)),
            ranges: vec![(offset, offset + 1)],
        });
        c.retrying(|| async {
            match sub.conn.call(&req, &c.rt, c.cfg.request_timeout).await {
                Ok(Response::Outcome(r)) if r.error == ErrorCode::NONE => Ok(()),
                Ok(Response::Outcome(r)) => Err(Attempt::Fail(ClientError::Broker { code: r.error, message: None })),
                Ok(other) => Err(Attempt::RetryMaybeDone(format!("unexpected response {other:?}"))),
                // Sent again on the same connection: the leader takes a
                // retry for what the transaction holds already.
                Err(CallError::Lost(why)) => Err(Attempt::RetryMaybeDone(why)),
                Err(CallError::NotSent(_)) => Err(Attempt::Fail(gone("the connection broke; the message went back"))),
                Err(e) => Err(e.into()),
            }
        })
        .await
    }

    /// Rejects a message: with `requeue` it is delivered again (to any
    /// consumer), without it is discarded like an acknowledged one.
    pub async fn nack(&self, message: &Message, requeue: bool) -> Result<(), ClientError> {
        let offset = message.offset();
        if !requeue {
            return self.client.retrying(|| self.settle_once(offset, Some(false))).await;
        }
        // Only the subscription's connection can give a message back; if it
        // is gone, so is the message (it went back with the subscription).
        let Some(sub) = &self.sub else { return Ok(()) };
        let req = Request::Nack(NackRequest {
            queue_id: self.resource_id,
            ranges: vec![(offset, offset + 1)],
            requeue: true,
            delay_ms: 0,
        });
        match sub.conn.call(&req, &self.client.rt, self.client.cfg.request_timeout).await {
            Ok(Response::Outcome(r)) if !r.error.retriable() && r.error != ErrorCode::NONE => {
                Err(ClientError::Broker { code: r.error, message: None })
            }
            // Not done (no leader, broken connection): the subscription ends
            // with it and the message goes back anyway.
            _ => Ok(()),
        }
    }

    /// One `Ack` (`nack: None`) or discarding `Nack` to the queue's leader.
    async fn settle_once(&self, offset: u64, nack: Option<bool>) -> Result<(), Attempt> {
        let c = self.client;
        let (route, addr) = c.route(&self.queue, 0).await?;
        let ranges = vec![(offset, offset + 1)];
        let req = match nack {
            None => Request::Ack(AckRequest::plain(route.resource_id, ranges)),
            Some(requeue) => Request::Nack(NackRequest { queue_id: route.resource_id, ranges, requeue, delay_ms: 0 }),
        };
        match c.call(&addr, &req).await {
            Ok(Response::Outcome(r)) => match r.error {
                ErrorCode::NONE => Ok(()),
                code if code.retriable() => {
                    c.redirect(&self.queue, 0, r.leader_hint);
                    Err(Attempt::RetryMaybeDone(code.to_string()))
                }
                code => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
            },
            Ok(other) => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            Err(e) => {
                c.forget(&self.queue, 0);
                Err(Attempt::RetryMaybeDone(e.to_string()))
            }
        }
    }

    /// Ends the subscription. Messages handed out stay with this consumer
    /// until acknowledged (or until this client's connection closes); the
    /// ones not handed out yet go back to the queue.
    pub async fn close(mut self) {
        if let Some((conn, reqs)) = self.cleanup() {
            for req in reqs {
                let _ = conn.send_only(&req).await;
            }
        }
    }

    /// The requests that end the subscription, and the connection to send
    /// them on.
    fn cleanup(&mut self) -> Option<Cleanup<R::Conn>> {
        let sub = self.sub.take()?;
        sub.conn.unregister(sub.id);
        let mut reqs = vec![Request::Unsubscribe(UnsubscribeRequest { subscription_id: sub.id })];
        let offsets: BTreeSet<u64> = self.buffered.drain(..).map(|m| m.offset()).collect();
        if !offsets.is_empty() {
            let ranges = offsets.iter().map(|o| (*o, o + 1)).collect();
            reqs.push(Request::Nack(NackRequest { queue_id: self.resource_id, ranges, requeue: true, delay_ms: 0 }));
        }
        Some((sub.conn, reqs))
    }
}

impl<R: Net + Clock + Spawn + Rng> Drop for Consumer<'_, R> {
    fn drop(&mut self) {
        if let Some((conn, reqs)) = self.cleanup() {
            // Detached: dropping the handle leaves the task running.
            drop(self.client.rt.spawn(async move {
                for req in reqs {
                    let _ = conn.send_only(&req).await;
                }
            }));
        }
    }
}
