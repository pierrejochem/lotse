use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use lotse_log::LogConfig;
use lotse_proto::{
    ApiKey, ErrorCode, ProducePartition, ProduceRequest, ProduceResource, Request, RequestHeader, Response,
};
use lotse_raft::{Config, Message, NodeId, Role};
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::Clock;
use lotse_sim::{Sim, SimRuntime};

use super::*;

#[derive(Debug, Default)]
struct Nothing;

impl StateMachine for Nothing {
    fn apply(&mut self, _: &Batch) {}
    fn snapshot(&self) -> Bytes {
        Bytes::new()
    }
    fn restore(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

const G: GroupId = 1;
type Groups = BTreeMap<GroupId, Replica<SimRuntime, Nothing>>;

/// Five replicas of group `G` with messages routed by hand.
struct Five {
    nodes: BTreeMap<NodeId, Groups>,
    queue: Vec<(NodeId, NodeId, Message)>,
}

impl Five {
    async fn new(rt: &SimRuntime) -> Five {
        Five::with(rt, crate::ReplicaOptions::default()).await
    }

    async fn with(rt: &SimRuntime, options: crate::ReplicaOptions) -> Five {
        let mut nodes = BTreeMap::new();
        for id in 1..=5 {
            let cfg = Config { pre_vote: false, ..Config::new(id, vec![1, 2, 3, 4, 5]) };
            let disk = rt.node(&format!("n{id}"));
            let r = Replica::open_with(disk, "g", cfg, Nothing, LogConfig::default(), options).await.unwrap();
            nodes.insert(id, BTreeMap::from([(G, r)]));
        }
        Five { nodes, queue: Vec::new() }
    }

    fn raft(&mut self, id: NodeId) -> &mut RaftGroup {
        self.nodes.get_mut(&id).unwrap().get_mut(&G).unwrap().raft()
    }

    fn get(&self, id: NodeId) -> &RaftGroup {
        self.nodes[&id][&G].raft_ref()
    }

    async fn process(&mut self, id: NodeId) {
        let out = self.nodes.get_mut(&id).unwrap().get_mut(&G).unwrap().process().await.unwrap();
        self.queue.extend(out.messages.into_iter().map(|s| (id, s.to, s.msg)));
    }

    /// Processes the nodes in `alive`, delivers what passes `link` and
    /// drops the rest, `rounds` times. `heartbeat` ticks that node once a
    /// round, so a leader tells followers its commit index.
    async fn run(
        &mut self,
        rounds: usize,
        alive: &[NodeId],
        heartbeat: Option<NodeId>,
        link: impl Fn(NodeId, NodeId) -> bool,
    ) {
        for _ in 0..rounds {
            if let Some(id) = heartbeat {
                self.raft(id).tick();
            }
            for &id in alive {
                self.process(id).await;
            }
            for (from, to, msg) in std::mem::take(&mut self.queue) {
                if alive.contains(&from) && alive.contains(&to) && link(from, to) {
                    self.raft(to).step(from, msg);
                }
            }
        }
    }

    /// Makes `id` campaign now (as on a leader's hand-over).
    fn campaign(&mut self, id: NodeId) {
        let term = self.get(id).term();
        self.raft(id).step(0, Message::TimeoutNow { term });
    }
}

fn produce_request(value: &str) -> Request {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    Request::Produce(ProduceRequest {
        txn_id: None,
        timeout_ms: 60_000,
        resources: vec![ProduceResource {
            resource_id: 7,
            partitions: vec![ProducePartition { partition: 0, batch: b.finish().unwrap() }],
        }],
    })
}

/// The same, stamped as batch `seq` of idempotent producer 42.
fn idempotent_request(value: &str, seq: i32) -> Request {
    let Request::Produce(mut req) = produce_request(value) else { unreachable!() };
    let batch = &mut req.resources[0].partitions[0].batch;
    let mut raw = bytes::BytesMut::from(&batch[..]);
    lotse_record::set_producer(&mut raw, 42, 0, seq).unwrap();
    *batch = raw.freeze();
    Request::Produce(req)
}

fn answers(outbox: &Outbox) -> Vec<Response> {
    let frames: Vec<Bytes> = outbox.queue.lock().unwrap().drain(..).collect();
    frames.into_iter().map(|f| lotse_proto::decode_response(f, ApiKey::PRODUCE).unwrap().1).collect()
}

/// The history only five voters allow: the leader's entry reaches one
/// follower, a new leader elected without both of them overwrites it on
/// the old leader, then dies; the follower that kept the entry wins and
/// commits it. The old leader saw its entry replaced, yet it was written:
/// answering "not written" at that point would be a lie.
#[test]
fn a_replaced_entry_is_not_reported_as_failed_until_the_commit_decides() {
    Sim::new(1).run(|rt| async move {
        let mut five = Five::new(&rt).await;
        let all = [1, 2, 3, 4, 5];
        five.campaign(1);
        five.run(6, &all, Some(1), |_, _| true).await;
        assert_eq!(five.get(1).role(), Role::Leader);
        assert_eq!(five.get(5).commit(), 1, "the leader's no-op is committed everywhere");

        // A produce reaches node 1; its entry (index 2) gets to node 2 only.
        let streams = [StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![G] }];
        let addrs = BTreeMap::new();
        let view =
            ClusterView { streams: &streams, queues: &[], client_addrs: &addrs, coordinator: None, now: rt.now() };
        let outbox = Arc::new(Outbox::default());
        let header = RequestHeader { api_key: ApiKey::PRODUCE, api_version: 0, correlation_id: 9, flags: 0 };
        let mut clients = Clients::default();
        let req = ClientRequest { outbox: outbox.clone(), header, request: produce_request("kept") };
        clients.handle(req, &view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        five.run(2, &all, None, |from, to| from == 1 && to == 2).await;
        assert_eq!(five.get(2).last_index(), 2, "node 2 holds the entry");
        assert_eq!(five.get(1).commit(), 1, "not committed: two of five");

        // Node 3 wins with nodes 4 and 5, which lack the entry, and
        // overwrites index 2 on node 1 (only) with its own no-op.
        five.campaign(3);
        five.run(1, &all, None, |from, to| from == 3 && (to == 4 || to == 5)).await;
        five.run(1, &all, None, |from, to| to == 3 && (from == 4 || from == 5)).await;
        assert_eq!(five.get(3).role(), Role::Leader);
        let t3 = five.get(3).term();
        five.run(6, &[1, 3], Some(3), |_, _| true).await;
        assert_eq!(five.get(1).log().term_at(2), Some(t3), "node 1's entry was replaced");
        assert_eq!(five.get(4).last_index(), 1, "nodes 4 and 5 have neither entry at index 2");
        clients.after_round(&view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        assert!(answers(&outbox).is_empty(), "no answer yet: node 2 may still commit the entry");

        // Node 3 dies. Node 2, which kept the entry, wins with 4 and 5 and
        // commits it; node 1 learns that.
        // Its first campaign is in the term 4 and 5 voted for node 3 in.
        five.campaign(2);
        five.run(2, &[1, 2, 4, 5], None, |_, _| true).await;
        five.campaign(2);
        five.run(8, &[1, 2, 4, 5], Some(2), |_, _| true).await;
        assert_eq!(five.get(2).role(), Role::Leader);
        assert!(five.get(1).commit() >= 2);
        clients.after_round(&view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        match &answers(&outbox)[..] {
            [Response::Produce(p)] => {
                let part = &p.resources[0].partitions[0];
                assert_eq!((part.error, part.base_offset), (ErrorCode::NONE, 1), "written, at offset 1");
            }
            other => panic!("expected one produce answer, got {other:?}"),
        }
    });
}

/// A retry of a batch that is still in flight is answered by the original's
/// fate, not at once: here the original is replaced, so neither request may
/// claim it was written.
#[test]
fn a_retry_of_a_batch_in_flight_waits_for_the_original() {
    Sim::new(2).run(|rt| async move {
        let mut five = Five::new(&rt).await;
        let all = [1, 2, 3, 4, 5];
        five.campaign(1);
        five.run(6, &all, Some(1), |_, _| true).await;
        assert_eq!(five.get(1).role(), Role::Leader);

        let streams = [StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![G] }];
        let addrs = BTreeMap::new();
        let view =
            ClusterView { streams: &streams, queues: &[], client_addrs: &addrs, coordinator: None, now: rt.now() };
        let header = |id| RequestHeader { api_key: ApiKey::PRODUCE, api_version: 0, correlation_id: id, flags: 0 };
        let mut clients = Clients::default();
        let outbox = Arc::new(Outbox::default());
        // The original reaches node 1 only; the client retries it there.
        for id in [1, 2] {
            let req = ClientRequest { outbox: outbox.clone(), header: header(id), request: idempotent_request("x", 0) };
            clients.handle(req, &view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        }
        five.run(1, &all, None, |_, _| false).await;
        assert_eq!(five.get(1).last_index(), 2, "proposed once: the retry is a duplicate");
        clients.after_round(&view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        assert!(answers(&outbox).is_empty(), "neither is answered while the original is in flight");

        // Node 3 wins without it and overwrites it on node 1, then commits.
        five.campaign(3);
        five.run(8, &[1, 3, 4, 5], Some(3), |_, _| true).await;
        assert_eq!(five.get(3).role(), Role::Leader);
        assert!(five.get(1).commit() >= 2);
        clients.after_round(&view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        let got = answers(&outbox);
        assert_eq!(got.len(), 2);
        for a in got {
            let Response::Produce(p) = a else { panic!("{a:?}") };
            assert_eq!(p.resources[0].partitions[0].error, ErrorCode::NOT_LEADER, "not written, so retriable");
        }
    });
}

/// One replica of group `G` that is its own quorum.
async fn single(rt: &SimRuntime) -> Replica<SimRuntime, Nothing> {
    let cfg = Config { pre_vote: false, ..Config::new(1, vec![1]) };
    Replica::open(rt.clone(), "g", cfg, Nothing, LogConfig::default()).await.unwrap()
}

/// Leads and applies an entry of its own term.
async fn lead(r: &mut Replica<SimRuntime, Nothing>) {
    for _ in 0..50 {
        r.raft().tick();
        r.process().await.unwrap();
        let raft = r.raft_ref();
        if raft.role() == Role::Leader && raft.log().term_at(r.applied()) == Some(raft.term()) {
            return;
        }
    }
    panic!("no leader");
}

fn transactional_batch(producer_id: u64, epoch: u16, seq: i32) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"in a transaction"), &[]);
    let mut raw = bytes::BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::set_producer(&mut raw, producer_id, epoch, seq).unwrap();
    lotse_record::set_transactional(&mut raw, true).unwrap();
    raw.freeze()
}

/// Right after a restart the commit index is known but nothing is applied
/// yet: the last stable offset must not run ahead of what is applied, or an
/// open transaction's data would reach read_committed consumers.
#[test]
fn after_a_restart_an_unapplied_open_transaction_stays_hidden() {
    Sim::new(3).run(|rt| async move {
        let mut r = single(&rt).await;
        lead(&mut r).await;
        r.propose(transactional_batch(5, 0, 0)).unwrap();
        r.process().await.unwrap();
        r.process().await.unwrap();
        drop(r);
        let mut r = single(&rt).await;
        while r.raft_ref().role() != Role::Leader {
            r.raft().tick();
        }
        assert!(r.raft_ref().commit() >= 2 && r.applied() == 0, "committed, not applied yet");
        let mut groups: Groups = BTreeMap::from([(G, r)]);
        let streams = [StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![G] }];
        let addrs = BTreeMap::new();
        let view =
            ClusterView { streams: &streams, queues: &[], client_addrs: &addrs, coordinator: None, now: rt.now() };
        let outbox = Arc::new(Outbox::default());
        let fetch = Request::Fetch(lotse_proto::FetchRequest {
            max_wait_ms: 0,
            min_bytes: 1,
            max_bytes: 1 << 20,
            isolation: lotse_proto::isolation::COMMITTED,
            resources: vec![lotse_proto::FetchResource {
                resource_id: 7,
                partitions: vec![lotse_proto::FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    leader_term: 0,
                    max_bytes: 1 << 20,
                }],
            }],
        });
        let header = RequestHeader { api_key: ApiKey::FETCH, api_version: 0, correlation_id: 1, flags: 0 };
        let mut clients = Clients::default();
        clients
            .handle(ClientRequest { outbox: outbox.clone(), header, request: fetch }, &view, &mut groups)
            .await
            .unwrap();
        let frame = outbox.queue.lock().unwrap().pop_front().expect("answered at once");
        let Response::Fetch(f) = lotse_proto::decode_response(frame, ApiKey::FETCH).unwrap().1 else { panic!() };
        let p = &f.resources[0].partitions[0];
        assert_eq!(p.error, ErrorCode::NONE);
        assert!(p.records.is_empty(), "nothing is stable before it is applied");
        assert_eq!(p.last_stable_offset, 0);
    });
}

/// A commit racing a timeout abort: while the abort's record is in flight,
/// the commit must be told to wait, not written as a second decision.
#[test]
fn a_commit_racing_a_timeout_abort_is_refused() {
    Sim::new(4).run(|rt| async move {
        let mut r = single(&rt).await;
        lead(&mut r).await;
        let mut groups: Groups = BTreeMap::from([(G, r)]);
        let streams = [StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![G] }];
        let addrs = BTreeMap::new();
        let mut now = rt.now();
        let mut clients = Clients::default();
        let outbox = Arc::new(Outbox::default());
        let mut id = 0;
        let mut ask = async |clients: &mut Clients, groups: &mut Groups, now, request: Request| {
            id += 1;
            let view = ClusterView { streams: &streams, queues: &[], client_addrs: &addrs, coordinator: Some(G), now };
            let header = RequestHeader { api_key: request.api_key(), api_version: 0, correlation_id: id, flags: 0 };
            let api_key = request.api_key();
            clients.handle(ClientRequest { outbox: outbox.clone(), header, request }, &view, groups).await.unwrap();
            for _ in 0..3 {
                groups.get_mut(&G).unwrap().process().await.unwrap();
                clients.after_round(&view, groups).await.unwrap();
            }
            let frame = outbox.queue.lock().unwrap().pop_front().expect("answered");
            lotse_proto::decode_response(frame, api_key).unwrap().1
        };
        let init =
            Request::InitProducer(lotse_proto::InitProducerRequest { txn_id: Some("t".into()), txn_timeout_ms: 100 });
        let Response::InitProducer(p) = ask(&mut clients, &mut groups, now, init).await else { panic!() };
        assert_eq!(p.error, ErrorCode::NONE);
        let add = Request::AddPartitionsToTxn(lotse_proto::AddPartitionsToTxnRequest {
            txn_id: "t".into(),
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            resources: vec![lotse_proto::TxnResource { resource_id: 7, partitions: vec![0] }],
        });
        let Response::AddPartitionsToTxn(a) = ask(&mut clients, &mut groups, now, add).await else { panic!() };
        assert_eq!(a.resources[0].partitions[0].error, ErrorCode::NONE);

        // The timeout passes: the coordinator proposes the abort, which is not
        // committed yet when the commit arrives.
        now = now + Duration::from_secs(1);
        let view = ClusterView { streams: &streams, queues: &[], client_addrs: &addrs, coordinator: Some(G), now };
        clients.after_round(&view, &mut groups).await.unwrap();
        assert!(clients.busy.contains("t"), "the abort is in flight");
        let end = Request::EndTxn(lotse_proto::EndTxnRequest {
            txn_id: "t".into(),
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            commit: true,
        });
        let Response::EndTxn(e) = ask(&mut clients, &mut groups, now, end).await else { panic!() };
        assert_eq!(e.error, ErrorCode::CONCURRENT_TRANSACTIONS, "not a second decision");
        let phase = groups[&G].coordinator().get("t").unwrap().phase;
        assert!(matches!(phase, crate::coordinator::Phase::PrepareAbort | crate::coordinator::Phase::CompleteAbort));
    });
}

/// Responses (not pushes) in `outbox`, decoded as answers to `key`; pushes
/// are returned separately.
fn drain(outbox: &Outbox, key: ApiKey) -> (Vec<Response>, Vec<lotse_proto::DeliverPush>) {
    let frames: Vec<Bytes> = outbox.queue.lock().unwrap().drain(..).collect();
    let (pushes, responses): (Vec<Bytes>, Vec<Bytes>) = frames.into_iter().partition(|f| lotse_proto::is_push(f));
    (
        responses.into_iter().map(|f| lotse_proto::decode_response(f, key).unwrap().1).collect(),
        pushes.into_iter().map(|f| lotse_proto::decode_push(f).unwrap()).collect(),
    )
}

/// Acks for offsets the queue's log does not hold yet are refused: they
/// would settle messages nobody has received.
#[test]
fn a_queue_refuses_acks_beyond_its_log_and_delivers_what_is_not_acked() {
    Sim::new(4).run(|rt| async move {
        let options = crate::ReplicaOptions { queue: true, ..crate::ReplicaOptions::default() };
        let cfg = Config::new(1, vec![1]);
        let r = Replica::open_with(rt.clone(), "q", cfg, Nothing, LogConfig::default(), options).await.unwrap();
        let mut groups: Groups = BTreeMap::from([(G, r)]);
        let replica = groups.get_mut(&G).unwrap();
        for _ in 0..40 {
            replica.raft().tick();
            replica.process().await.unwrap();
        }
        let queues = [QueueConfig {
            name: "jobs".into(),
            resource_id: 7,
            group: G,
            ack_timeout: QueueConfig::DEFAULT_ACK_TIMEOUT,
        }];
        let addrs = BTreeMap::new();
        let view =
            ClusterView { streams: &[], queues: &queues, client_addrs: &addrs, coordinator: None, now: rt.now() };
        let outbox = Arc::new(Outbox::default());
        let mut clients = Clients::default();
        let send = async |clients: &mut Clients, groups: &mut Groups, key, request| {
            let header = RequestHeader { api_key: key, api_version: 0, correlation_id: 5, flags: 0 };
            clients.handle(ClientRequest { outbox: outbox.clone(), header, request }, &view, groups).await.unwrap();
            for _ in 0..3 {
                groups.get_mut(&G).unwrap().process().await.unwrap();
                clients.after_round(&view, groups).await.unwrap();
            }
            drain(&outbox, key)
        };
        for v in ["a", "b"] {
            send(&mut clients, &mut groups, ApiKey::PRODUCE, produce_request(v)).await; // offsets 1, 2
        }
        let ack = |ranges| Request::Ack(lotse_proto::AckRequest { queue_id: 7, txn_id: None, ranges });
        let outcome = |r: &[Response]| match r {
            [Response::Outcome(o)] => o.error,
            other => panic!("expected one outcome, got {other:?}"),
        };
        let (r, _) = send(&mut clients, &mut groups, ApiKey::ACK, ack(vec![(2, 10)])).await;
        assert_eq!(outcome(&r), ErrorCode::INVALID_REQUEST, "offsets 3..10 are not in the log");
        let (r, _) = send(&mut clients, &mut groups, ApiKey::ACK, ack(vec![(1, 2)])).await;
        assert_eq!(outcome(&r), ErrorCode::NONE);

        let sub = Request::Subscribe(lotse_proto::SubscribeRequest {
            queue_id: 7,
            consumer_tag: "w".into(),
            credit_msgs: 10,
            credit_bytes: 1 << 20,
            exclusive: false,
        });
        let (r, pushes) = send(&mut clients, &mut groups, ApiKey::SUBSCRIBE, sub).await;
        assert!(matches!(&r[..], [Response::Subscribe(s)] if s.error == ErrorCode::NONE), "{r:?}");
        let delivered: Vec<u64> = pushes.iter().flat_map(|p| p.deliveries.iter().map(|d| d.offset)).collect();
        assert_eq!(delivered, vec![2], "only the message that was not acknowledged");
    });
}

/// A new leader may hold an ack that its predecessor committed without
/// knowing it is committed: it must not deliver until it has applied an
/// entry of its own term, or the acknowledged message goes out again.
#[test]
fn a_new_queue_leader_delivers_only_after_applying_its_own_term() {
    Sim::new(5).run(|rt| async move {
        let options = crate::ReplicaOptions { queue: true, ..crate::ReplicaOptions::default() };
        let mut five = Five::with(&rt, options).await;
        let all = [1, 2, 3, 4, 5];
        five.campaign(1);
        five.run(6, &all, Some(1), |_, _| true).await;
        assert_eq!(five.get(1).role(), Role::Leader);
        let queues = [QueueConfig {
            name: "jobs".into(),
            resource_id: 7,
            group: G,
            ack_timeout: QueueConfig::DEFAULT_ACK_TIMEOUT,
        }];
        let addrs = BTreeMap::new();
        let view =
            ClusterView { streams: &[], queues: &queues, client_addrs: &addrs, coordinator: None, now: rt.now() };
        let outbox = Arc::new(Outbox::default());
        let header = |key| RequestHeader { api_key: key, api_version: 0, correlation_id: 3, flags: 0 };
        let mut c1 = Clients::default();

        // Message "a" at offset 1, committed and known committed everywhere.
        let req =
            ClientRequest { outbox: outbox.clone(), header: header(ApiKey::PRODUCE), request: produce_request("a") };
        c1.handle(req, &view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        five.run(6, &all, Some(1), |_, _| true).await;
        assert_eq!(five.get(2).commit(), 2);

        // Its ack reaches nodes 2 and 3 and commits on node 1; nobody else
        // learns that it is committed.
        let ack = Request::Ack(lotse_proto::AckRequest { queue_id: 7, txn_id: None, ranges: vec![(1, 2)] });
        let req = ClientRequest { outbox: outbox.clone(), header: header(ApiKey::ACK), request: ack };
        c1.handle(req, &view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        let near = |f: NodeId, t: NodeId| (f == 1 && (t == 2 || t == 3)) || ((f == 2 || f == 3) && t == 1);
        five.run(2, &all, None, near).await;
        assert_eq!(five.get(1).commit(), 3, "the ack is committed on node 1");
        assert_eq!((five.get(2).last_index(), five.get(2).commit()), (3, 2), "node 2 holds it, not known committed");

        // Node 2 wins with 3 and 4 and has a consumer before its no-op commits.
        five.campaign(2);
        five.run(1, &all, None, |f, t| f == 2 && (t == 3 || t == 4)).await;
        five.run(1, &all, None, |f, t| t == 2 && (f == 3 || f == 4)).await;
        assert_eq!(five.get(2).role(), Role::Leader);
        let mut c2 = Clients::default();
        let sub = Request::Subscribe(lotse_proto::SubscribeRequest {
            queue_id: 7,
            consumer_tag: "w".into(),
            credit_msgs: 10,
            credit_bytes: 1 << 20,
            exclusive: false,
        });
        let req = ClientRequest { outbox: outbox.clone(), header: header(ApiKey::SUBSCRIBE), request: sub };
        let groups = five.nodes.get_mut(&2).unwrap();
        c2.handle(req, &view, groups).await.unwrap();
        groups.get_mut(&G).unwrap().process().await.unwrap();
        c2.after_round(&view, groups).await.unwrap();
        let (_, pushes) = drain(&outbox, ApiKey::SUBSCRIBE);
        assert!(pushes.is_empty(), "delivered before its own term is applied: {pushes:?}");

        // Once its no-op commits, the ack is applied and nothing is left.
        five.run(6, &[1, 2, 3, 4, 5], Some(2), |_, _| true).await;
        c2.after_round(&view, five.nodes.get_mut(&2).unwrap()).await.unwrap();
        let (_, pushes) = drain(&outbox, ApiKey::SUBSCRIBE);
        assert!(pushes.is_empty(), "the acknowledged message came back: {pushes:?}");
    });
}
