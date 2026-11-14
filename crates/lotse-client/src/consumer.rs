//! Consuming a queue: [`Client::subscribe`] and [`Consumer`].
//!
//! A subscription lives on one connection to the queue's leader. The leader
//! pushes batches as stored with the offsets in them that are this
//! subscription's, never more messages than the consumer has credit for.
//! The consumer hands them out one at a time and adds credit as they are
//! used up, so at most `credit` messages wait unprocessed on this side.
//!
//! When the subscription ends the consumer subscribes again on the next
//! call. The leader gave it a consumer id with the first subscription
//! (ADR 0109), which it names when it subscribes again: after a failover
//! or a broken connection the messages it holds are still its own at the
//! new subscription, so it keeps the ones waiting here, and acks them,
//! within a transaction too, through the new one. Only after the ack
//! timeout, or when the consumer did not come back within the queue's
//! resume window, did the leader give them to others; after the ack timeout
//! the ones waiting here are dropped. Acknowledging a message that went to
//! someone else meanwhile still settles it: an ack names an offset, not a
//! delivery.

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
    /// The subscription it came on: a requeue names it, so that it takes
    /// back only this delivery (ADR 0106).
    subscription: u32,
}

impl Message {
    /// The message's offset in the queue.
    pub fn offset(&self) -> u64 {
        self.record.offset
    }
}

/// What ending a subscription takes: see [`Consumer::cleanup`].
struct Cleanup<C: lotse_rt::Conn> {
    conn: Arc<Connection<C>>,
    queue_id: u64,
    sub: u32,
    pushes: PushQueue,
    /// Messages here that were never handed out.
    offsets: BTreeSet<u64>,
}

struct Sub<C: lotse_rt::Conn> {
    conn: Arc<Connection<C>>,
    id: u32,
    pushes: PushQueue,
    /// Waiting for its turn as the queue's single active consumer.
    standby: bool,
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
    /// Delivered to this consumer, not handed out yet.
    buffered: VecDeque<Message>,
    /// The id the leader gave this consumer, which it resumes with (ADR
    /// 0109); 0 until one is given, or from a node that gives none.
    consumer_id: u64,
    /// Requeues `(offset, delay ms)` that did not reach the leader because
    /// it moved or the connection broke: the messages are still this
    /// consumer's at the new subscription, which gives them back.
    unsent: std::sync::Mutex<Vec<(u64, u32)>>,
    /// Messages and bytes used since credit was last added.
    used: (u32, u32),
    /// How often it has subscribed.
    subscribed: u64,
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
            consumer_id: 0,
            unsent: std::sync::Mutex::new(Vec::new()),
            used: (0, 0),
            subscribed: 0,
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
            let (sub, resource_id, consumer_id) = self.client.retrying(|| self.subscribe_once()).await?;
            if consumer_id != 0 {
                self.consumer_id = consumer_id;
            }
            // Requeues that missed the old subscription go on the new one,
            // which took their messages back (ADR 0109).
            let unsent = std::mem::take(&mut *self.unsent.lock().expect("unsent lock"));
            for (offset, delay_ms) in unsent {
                let nack = Request::Nack(NackRequest {
                    queue_id: resource_id,
                    ranges: vec![(offset, offset + 1)],
                    requeue: true,
                    delay_ms,
                    subscription_id: Some(sub.id),
                });
                let _ = sub.conn.send_only(&nack).await;
            }
            self.sub = Some(sub);
            self.subscribed += 1;
            self.resource_id = resource_id;
            self.used = (0, 0);
        }
        Ok(())
    }

    async fn subscribe_once(&self) -> Result<(Sub<R::Conn>, u64, u64), Attempt> {
        let c = self.client;
        let (route, addr) = c.route(&self.queue, 0).await?;
        let conn = match c.connection(&addr).await {
            Ok(conn) => conn,
            Err(e) => {
                // The leader it knew may be gone: ask again where it is.
                c.forget(&self.queue, 0);
                return Err(Attempt::Retry(e.to_string()));
            }
        };
        let req = Request::Subscribe(SubscribeRequest {
            queue_id: route.resource_id,
            consumer_tag: String::new(),
            credit_msgs: self.credit,
            credit_bytes: u32::MAX,
            exclusive: false,
            notify_active: true,
            resume: Some(self.consumer_id).filter(|c| *c != 0),
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
                Some(pushes) => {
                    let sub = Sub { conn, id: resp.subscription_id, pushes, standby: resp.standby };
                    Ok((sub, route.resource_id, resp.consumer_id))
                }
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
                Some(Err(_broken)) => self.ended(true)?,
                Some(Ok(push)) if push.error != ErrorCode::NONE => {
                    if push.error == ErrorCode::NOT_LEADER {
                        self.client.redirect(&self.queue, 0, push.leader_hint);
                    } else if push.error == ErrorCode::UNKNOWN_RESOURCE {
                        self.client.forget(&self.queue, 0);
                    }
                    // A failover leaves this consumer its messages; the ack
                    // timeout, or a queue that is gone, does not.
                    self.ended(push.error == ErrorCode::NOT_LEADER)?;
                }
                Some(Ok(push)) if push.is_activation() => {
                    if let Some(sub) = &mut self.sub {
                        sub.standby = false;
                    }
                }
                Some(Ok(push)) => {
                    self.buffer(&push)?;
                    self.used.1 = self.used.1.saturating_add(push.batch.len() as u32);
                    // Messages come to the active consumer only.
                    if let Some(sub) = &mut self.sub {
                        sub.standby = false;
                    }
                }
            }
        }
    }

    /// How often this consumer has subscribed: it subscribes again after a
    /// subscription ends (a failover, the ack timeout, a broken
    /// connection), and messages it had go to others then.
    pub fn subscriptions(&self) -> u64 {
        self.subscribed
    }

    /// Whether the subscription waits as a standby of its queue's single
    /// active consumer (ADR 0076): subscribed, and not its turn yet. A
    /// standby that becomes active is told; this is false again from the
    /// next [`Consumer::next`] on that sees it.
    pub fn is_standby(&self) -> bool {
        self.sub.as_ref().is_some_and(|s| s.standby)
    }

    /// Keeps the messages of a delivery push that are this consumer's.
    fn buffer(&mut self, push: &lotse_proto::DeliverPush) -> Result<(), ClientError> {
        let corrupt = |e: lotse_record::Error| ClientError::Broker {
            code: ErrorCode::CORRUPT_BATCH,
            message: Some(e.to_string()),
        };
        let (batch, _) = lotse_record::Batch::parse(&push.batch).map_err(corrupt)?;
        let mine: std::collections::BTreeMap<u64, u16> =
            push.deliveries.iter().map(|d| (d.offset, d.delivery_count)).collect();
        for record in batch.records().map_err(corrupt)? {
            if let Some(&delivery_count) = mine.get(&record.offset) {
                let subscription = push.subscription_id;
                self.buffered.push_back(Message { record, delivery_count, subscription });
            }
        }
        Ok(())
    }

    /// The subscription is over. If the leader gave this consumer an id and
    /// it ended for a failover or a broken connection (`resumable`), its
    /// messages are still its own when it subscribes again (ADR 0109): the
    /// ones waiting here stay, with those pushed but not read yet.
    /// Otherwise they went to others and are dropped.
    fn ended(&mut self, resumable: bool) -> Result<(), ClientError> {
        let Some(sub) = self.sub.take() else { return Ok(()) };
        sub.conn.unregister(sub.id);
        if !(resumable && self.consumer_id != 0) {
            self.buffered.clear();
            return Ok(());
        }
        let unread: Vec<_> = sub.pushes.lock().expect("push lock").0.drain(..).flatten().collect();
        for push in unread {
            if push.error == ErrorCode::NONE && !push.is_activation() {
                self.buffer(&push)?;
            }
        }
        Ok(())
    }

    /// The subscription to send a nack or a transactional ack of `message`
    /// on: its own, or, once the leader gave this consumer an id, the
    /// current one, which took back what earlier ones held (ADR 0109).
    fn sub_for(&self, message: &Message) -> Option<&Sub<R::Conn>> {
        self.sub.as_ref().filter(|s| s.id == message.subscription || self.consumer_id != 0)
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
    /// transaction first. Only a message this consumer holds can be
    /// acknowledged so; any error leaves the transaction to abort.
    ///
    /// The ack goes on the subscription's connection. If the leader changed
    /// or the connection broke, the consumer subscribes again, naming its
    /// id, and sends it on the new subscription, which holds the message
    /// still (ADR 0109); only if the message went to someone else meanwhile
    /// is it `MESSAGE_NOT_HELD`.
    pub async fn ack_in(&mut self, txn: &mut Transaction<'_, R>, message: &Message) -> Result<(), ClientError> {
        txn.add(&self.queue, 0).await?;
        let res = self.ack_in_resuming(txn, message).await;
        if let Err(e) = &res {
            txn.poisoned = Some(e.to_string());
        }
        res
    }

    async fn ack_in_resuming(&mut self, txn: &Transaction<'_, R>, message: &Message) -> Result<(), ClientError> {
        let gone = |why: &str| ClientError::Broker { code: ErrorCode::MESSAGE_NOT_HELD, message: Some(why.into()) };
        let deadline = self.client.rt.now() + self.client.cfg.retry_for;
        loop {
            if self.sub.is_none() && self.consumer_id != 0 {
                self.ensure_subscribed().await?;
            }
            // Without an id the message is held only on its subscription's
            // connection, which is gone if this is another one.
            let Some(sub) = self.sub_for(message) else {
                return Err(gone("the subscription ended; the message went back"));
            };
            let resumable = self.consumer_id != 0;
            match self.ack_in_once(sub, txn, message.offset()).await {
                Ok(()) => return Ok(()),
                // The leader moved or the connection broke: subscribe again,
                // as this consumer, and send it there.
                // (The connection is shared: only the subscription goes.)
                Err(Resend(_)) if resumable && self.client.rt.now() < deadline => {
                    self.client.forget(&self.queue, 0);
                    self.ended(true)?;
                }
                Err(Resend(why)) => return Err(gone(&why)),
                Err(Done(e)) => return Err(e),
            }
        }
    }

    async fn ack_in_once(&self, sub: &Sub<R::Conn>, txn: &Transaction<'_, R>, offset: u64) -> Result<(), AckInError> {
        let c = self.client;
        let req = Request::Ack(AckRequest {
            queue_id: self.resource_id,
            txn_id: Some(txn.txn_id.clone()),
            tenant: c.cfg.tenant.clone(),
            producer: Some((txn.producer_id, txn.epoch)),
            ranges: vec![(offset, offset + 1)],
        });
        let mut lost = 0;
        loop {
            match sub.conn.call(&req, &c.rt, c.cfg.request_timeout).await {
                Ok(Response::Outcome(r)) if r.error == ErrorCode::NONE => return Ok(()),
                Ok(Response::Outcome(r)) if r.error.retriable() => return Err(Resend(r.error.to_string())),
                Ok(Response::Outcome(r)) => return Err(Done(ClientError::Broker { code: r.error, message: None })),
                Ok(other) => return Err(Resend(format!("unexpected response {other:?}"))),
                // Sent again on the same connection first: the leader takes
                // a retry for what the transaction holds already.
                Err(CallError::Lost(_)) if lost < 2 => lost += 1,
                Err(e) => return Err(Resend(e.to_string())),
            }
        }
    }

    /// Rejects a message: with `requeue` it is delivered again (to any
    /// consumer), without it is discarded like an acknowledged one.
    pub async fn nack(&self, message: &Message, requeue: bool) -> Result<(), ClientError> {
        if !requeue {
            return self.client.retrying(|| self.settle_once(message.offset(), Some(false))).await;
        }
        self.requeue(message, 0).await
    }

    /// Gives a message back to be delivered again, to any consumer, once
    /// `delay` has passed (ADR 0077): a retry with backoff. Rounded down to
    /// whole ms, at most about 49 days; a delay of 0 is [`Consumer::nack`]
    /// with `requeue`. If the subscription has ended, the message went back
    /// with it, without a delay.
    pub async fn nack_after(&self, message: &Message, delay: Duration) -> Result<(), ClientError> {
        let ms = u32::try_from(delay.as_millis()).unwrap_or(u32::MAX);
        self.requeue(message, ms).await
    }

    async fn requeue(&self, message: &Message, delay_ms: u32) -> Result<(), ClientError> {
        // Only the subscription's connection can give a message back; if it
        // is gone, the message went back with it, or waits at the new leader
        // for this consumer, which takes it back when it subscribes again and
        // gives it back from there.
        let Some(sub) = self.sub_for(message) else { return Ok(()) };
        let offset = message.offset();
        let req = Request::Nack(NackRequest {
            queue_id: self.resource_id,
            ranges: vec![(offset, offset + 1)],
            requeue: true,
            delay_ms,
            // A message from an earlier subscription went back when that
            // ended, and a newer one may hold it again now and keeps it;
            // unless this consumer resumed it there (ADR 0109).
            subscription_id: Some(sub.id),
        });
        match sub.conn.call(&req, &self.client.rt, self.client.cfg.request_timeout).await {
            Ok(Response::Outcome(r)) if r.error == ErrorCode::NONE => Ok(()),
            Ok(Response::Outcome(r)) if !r.error.retriable() => {
                Err(ClientError::Broker { code: r.error, message: None })
            }
            // Not done (no leader, broken connection): the subscription ends
            // with it. Without an id the message goes back with it; with one
            // it stays this consumer's, and the next subscription gives it
            // back.
            _ => {
                if self.consumer_id != 0 {
                    self.unsent.lock().expect("unsent lock").push((offset, delay_ms));
                }
                Ok(())
            }
        }
    }

    /// One `Ack` (`nack: None`) or discarding `Nack` to the queue's leader.
    async fn settle_once(&self, offset: u64, nack: Option<bool>) -> Result<(), Attempt> {
        let c = self.client;
        let (route, addr) = c.route(&self.queue, 0).await?;
        let ranges = vec![(offset, offset + 1)];
        let req = match nack {
            None => Request::Ack(AckRequest::plain(route.resource_id, ranges)),
            Some(requeue) => Request::Nack(NackRequest {
                queue_id: route.resource_id,
                ranges,
                requeue,
                delay_ms: 0,
                subscription_id: None,
            }),
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
    /// ones not handed out yet, also those pushed but not read, go back to
    /// the queue.
    pub async fn close(mut self) {
        if let Some(c) = self.cleanup() {
            finish(c, self.client.rt.clone(), self.client.cfg.request_timeout).await;
        }
    }

    /// Takes the subscription for ending it, with the messages here that
    /// were never handed out.
    fn cleanup(&mut self) -> Option<Cleanup<R::Conn>> {
        let sub = self.sub.take()?;
        let offsets = self.buffered.drain(..).map(|m| m.offset()).collect();
        Some(Cleanup { conn: sub.conn, queue_id: self.resource_id, sub: sub.id, pushes: sub.pushes, offsets })
    }
}

/// Ends a subscription: unsubscribes and waits for the answer, then gives
/// back what was never handed out. Pushes and answers come in order on the
/// connection, so every push the leader sent before it took the
/// unsubscribe has arrived by then (the leader writes a checkout before it
/// pushes, ADR 0109, so one may still be on its way when the consumer ends);
/// the leader counts their messages as in flight with the subscription, and
/// without a nack they would wait for the ack timeout. Nothing reads the
/// pushes after this, so all of them are taken.
async fn finish<R: Clock, C: lotse_rt::Conn>(c: Cleanup<C>, rt: R, timeout: Duration) {
    let Cleanup { conn, queue_id, sub, pushes, mut offsets } = c;
    let unsubscribe = Request::Unsubscribe(UnsubscribeRequest { subscription_id: sub });
    let _ = conn.call(&unsubscribe, &rt, timeout).await;
    conn.unregister(sub);
    {
        let mut q = pushes.lock().expect("push lock");
        for push in q.0.drain(..).flatten() {
            if push.error == ErrorCode::NONE && !push.is_activation() {
                offsets.extend(push.deliveries.iter().map(|d| d.offset));
            }
        }
    }
    if !offsets.is_empty() {
        let ranges = offsets.iter().map(|o| (*o, o + 1)).collect();
        // Named, so that a newer subscription on this connection keeps what
        // it received since, should this arrive late (ADR 0106).
        let nack =
            Request::Nack(NackRequest { queue_id, ranges, requeue: true, delay_ms: 0, subscription_id: Some(sub) });
        let _ = conn.send_only(&nack).await;
    }
}

/// Why a transactional ack did not go through on a subscription.
enum AckInError {
    /// The leader moved or the connection broke: worth sending again on a
    /// new subscription.
    Resend(String),
    /// The leader's answer.
    Done(ClientError),
}
use AckInError::{Done, Resend};

impl<R: Net + Clock + Spawn + Rng> Drop for Consumer<'_, R> {
    fn drop(&mut self) {
        if let Some(c) = self.cleanup() {
            let (rt, timeout) = (self.client.rt.clone(), self.client.cfg.request_timeout);
            // Detached: dropping the handle leaves the task running.
            drop(self.client.rt.spawn(async move { finish(c, rt, timeout).await }));
        }
    }
}
