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

/// Everyone may do everything: these tests are about other things.
static OPEN: std::sync::LazyLock<crate::access::Access> = std::sync::LazyLock::new(|| {
    crate::access::Access::build(&crate::AuthConfig::none(), &[], &crate::metadata::MetadataState::default())
});

static NOTHING_DELETED: BTreeSet<u64> = BTreeSet::new();

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
        let view = ClusterView {
            streams: &streams,
            queues: &[],
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(45),
            group_heartbeat: Duration::from_secs(5),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: None,
            node: NodeView::default(),
            now: rt.now(),
            unix_ms: 0,
            access: &OPEN,
        };
        let outbox = Arc::new(Outbox::default());
        let header = RequestHeader { api_key: ApiKey::PRODUCE, api_version: 0, correlation_id: 9, flags: 0 };
        let mut clients = Clients::default();
        let req = ClientRequest {
            outbox: outbox.clone(),
            header,
            request: produce_request("kept"),
            principal: Principal::Anonymous,
        };
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
        let view = ClusterView {
            streams: &streams,
            queues: &[],
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(45),
            group_heartbeat: Duration::from_secs(5),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: None,
            node: NodeView::default(),
            now: rt.now(),
            unix_ms: 0,
            access: &OPEN,
        };
        let header = |id| RequestHeader { api_key: ApiKey::PRODUCE, api_version: 0, correlation_id: id, flags: 0 };
        let mut clients = Clients::default();
        let outbox = Arc::new(Outbox::default());
        // The original reaches node 1 only; the client retries it there.
        for id in [1, 2] {
            let req = ClientRequest {
                outbox: outbox.clone(),
                header: header(id),
                request: idempotent_request("x", 0),
                principal: Principal::Anonymous,
            };
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
        let view = ClusterView {
            streams: &streams,
            queues: &[],
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(45),
            group_heartbeat: Duration::from_secs(5),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: None,
            node: NodeView::default(),
            now: rt.now(),
            unix_ms: 0,
            access: &OPEN,
        };
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
            .handle(
                ClientRequest { outbox: outbox.clone(), header, request: fetch, principal: Principal::Anonymous },
                &view,
                &mut groups,
            )
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
            let view = ClusterView {
                streams: &streams,
                queues: &[],
                exchanges: &[],
                deleted: &NOTHING_DELETED,
                group_session: Duration::from_secs(45),
                group_heartbeat: Duration::from_secs(5),
                admin: AdminView::default(),
                client_addrs: &addrs,
                coordinator: Some(G),
                node: NodeView::default(),
                now,
                unix_ms: 0,
                access: &OPEN,
            };
            let header = RequestHeader { api_key: request.api_key(), api_version: 0, correlation_id: id, flags: 0 };
            let api_key = request.api_key();
            clients
                .handle(
                    ClientRequest { outbox: outbox.clone(), header, request, principal: Principal::Anonymous },
                    &view,
                    groups,
                )
                .await
                .unwrap();
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
        let view = ClusterView {
            streams: &streams,
            queues: &[],
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(45),
            group_heartbeat: Duration::from_secs(5),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: Some(G),
            node: NodeView::default(),
            now,
            unix_ms: 0,
            access: &OPEN,
        };
        clients.after_round(&view, &mut groups).await.unwrap();
        assert!(clients.busy.contains(&coord::Busy::Txn("t".into())), "the abort is in flight");
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
            limits: Default::default(),
            dead_letter: None,
        }];
        let addrs = BTreeMap::new();
        let view = ClusterView {
            streams: &[],
            queues: &queues,
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(45),
            group_heartbeat: Duration::from_secs(5),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: None,
            node: NodeView::default(),
            now: rt.now(),
            unix_ms: 0,
            access: &OPEN,
        };
        let outbox = Arc::new(Outbox::default());
        let mut clients = Clients::default();
        let send = async |clients: &mut Clients, groups: &mut Groups, key, request| {
            let header = RequestHeader { api_key: key, api_version: 0, correlation_id: 5, flags: 0 };
            clients
                .handle(
                    ClientRequest { outbox: outbox.clone(), header, request, principal: Principal::Anonymous },
                    &view,
                    groups,
                )
                .await
                .unwrap();
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
            limits: Default::default(),
            dead_letter: None,
        }];
        let addrs = BTreeMap::new();
        let view = ClusterView {
            streams: &[],
            queues: &queues,
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(45),
            group_heartbeat: Duration::from_secs(5),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: None,
            node: NodeView::default(),
            now: rt.now(),
            unix_ms: 0,
            access: &OPEN,
        };
        let outbox = Arc::new(Outbox::default());
        let header = |key| RequestHeader { api_key: key, api_version: 0, correlation_id: 3, flags: 0 };
        let mut c1 = Clients::default();

        // Message "a" at offset 1, committed and known committed everywhere.
        let req = ClientRequest {
            outbox: outbox.clone(),
            header: header(ApiKey::PRODUCE),
            request: produce_request("a"),
            principal: Principal::Anonymous,
        };
        c1.handle(req, &view, five.nodes.get_mut(&1).unwrap()).await.unwrap();
        five.run(6, &all, Some(1), |_, _| true).await;
        assert_eq!(five.get(2).commit(), 2);

        // Its ack reaches nodes 2 and 3 and commits on node 1; nobody else
        // learns that it is committed.
        let ack = Request::Ack(lotse_proto::AckRequest { queue_id: 7, txn_id: None, ranges: vec![(1, 2)] });
        let req = ClientRequest {
            outbox: outbox.clone(),
            header: header(ApiKey::ACK),
            request: ack,
            principal: Principal::Anonymous,
        };
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
        let req = ClientRequest {
            outbox: outbox.clone(),
            header: header(ApiKey::SUBSCRIBE),
            request: sub,
            principal: Principal::Anonymous,
        };
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

/// A member that has not released a revoked partition within its
/// rebalance timeout is removed, but never sooner than the session timeout
/// after the revocation: it may not have heard of the revocation yet, and
/// only after a session without a successful heartbeat is it sure to have
/// stopped reading.
#[test]
fn a_member_slow_to_release_is_removed_no_sooner_than_a_session() {
    Sim::new(6).run(|rt| async move {
        let cfg = Config::new(1, vec![1]);
        let r = Replica::open(rt.clone(), "c", cfg, Nothing, LogConfig::default()).await.unwrap();
        let mut groups: Groups = BTreeMap::from([(G, r)]);
        let replica = groups.get_mut(&G).unwrap();
        for _ in 0..40 {
            replica.raft().tick();
            replica.process().await.unwrap();
        }
        let streams = [StreamConfig { name: "in".into(), resource_id: 7, partitions: vec![2, 3] }];
        let addrs = BTreeMap::new();
        let start = rt.now();
        let view_at = |ms: u64| ClusterView {
            streams: &streams,
            queues: &[],
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(1),
            group_heartbeat: Duration::from_millis(100),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: Some(G),
            node: NodeView::default(),
            now: start + Duration::from_millis(ms),
            unix_ms: 0,
            access: &OPEN,
        };
        let outbox = Arc::new(Outbox::default());
        let mut clients = Clients::default();
        let heartbeat =
            async |clients: &mut Clients, groups: &mut Groups, ms, member: &str, epoch, owned: Option<Vec<u32>>| {
                let request = Request::GroupHeartbeat(lotse_proto::GroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: member.into(),
                    member_epoch: epoch,
                    rebalance_timeout_ms: 100,
                    subscribed: (epoch == 0).then(|| vec![7]),
                    owned: owned.map(|p| vec![lotse_proto::TxnResource { resource_id: 7, partitions: p }]),
                });
                let header =
                    RequestHeader { api_key: ApiKey::GROUP_HEARTBEAT, api_version: 0, correlation_id: 1, flags: 0 };
                let view = view_at(ms);
                clients
                    .handle(
                        ClientRequest { outbox: outbox.clone(), header, request, principal: Principal::Anonymous },
                        &view,
                        groups,
                    )
                    .await
                    .unwrap();
                for _ in 0..3 {
                    groups.get_mut(&G).unwrap().process().await.unwrap();
                    clients.after_round(&view, groups).await.unwrap();
                }
                match &drain(&outbox, ApiKey::GROUP_HEARTBEAT).0[..] {
                    [Response::GroupHeartbeat(h)] => h.clone(),
                    other => panic!("expected one heartbeat answer, got {other:?}"),
                }
            };
        let a = heartbeat(&mut clients, &mut groups, 0, "", 0, None).await;
        assert_eq!(a.assignment.as_ref().map(|r| r[0].partitions.len()), Some(2), "alone, it gets both");
        let b = heartbeat(&mut clients, &mut groups, 10, "", 0, None).await;
        assert_eq!(b.assignment.as_ref().map(|r| r.len()), Some(0), "nothing free yet");
        // A learns that one partition is revoked, and keeps reading it.
        let a2 = heartbeat(&mut clients, &mut groups, 20, &a.member_id, a.member_epoch, Some(vec![0, 1])).await;
        assert_eq!(a2.assignment.as_ref().map(|r| r[0].partitions.len()), Some(1));
        let members = |groups: &Groups| -> Vec<String> {
            let g = groups[&G].coordinator().groups().groups.get("g").cloned().unwrap_or_default();
            g.members.into_keys().collect()
        };
        // Past its rebalance timeout, within a session: still a member.
        for ms in [300, 600, 900] {
            heartbeat(&mut clients, &mut groups, ms, &a.member_id, a2.member_epoch, Some(vec![0, 1])).await;
            clients.after_round(&view_at(ms + 50), &mut groups).await.unwrap();
            assert!(members(&groups).contains(&a.member_id), "removed {ms} ms into a revocation");
        }
        // A session after the revocation, with the partition still held: out.
        heartbeat(&mut clients, &mut groups, 1_100, &a.member_id, a2.member_epoch, Some(vec![0, 1])).await;
        let view = view_at(1_100);
        for _ in 0..3 {
            clients.after_round(&view, &mut groups).await.unwrap();
            groups.get_mut(&G).unwrap().process().await.unwrap();
        }
        assert!(!members(&groups).contains(&a.member_id), "kept although it never released");
    });
}

/// Offsets are accepted only from a member that still holds the partition:
/// once it has released one, its commits for it are refused, and a commit
/// refused in part writes nothing.
#[test]
fn offsets_for_a_released_partition_are_refused() {
    Sim::new(7).run(|rt| async move {
        let cfg = Config::new(1, vec![1]);
        let r = Replica::open(rt.clone(), "c", cfg, Nothing, LogConfig::default()).await.unwrap();
        let mut groups: Groups = BTreeMap::from([(G, r)]);
        let replica = groups.get_mut(&G).unwrap();
        for _ in 0..40 {
            replica.raft().tick();
            replica.process().await.unwrap();
        }
        let streams = [StreamConfig { name: "in".into(), resource_id: 7, partitions: vec![2, 3] }];
        let addrs = BTreeMap::new();
        let start = rt.now();
        let view_at = |ms: u64| ClusterView {
            streams: &streams,
            queues: &[],
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(1),
            group_heartbeat: Duration::from_millis(100),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: Some(G),
            node: NodeView::default(),
            now: start + Duration::from_millis(ms),
            unix_ms: 0,
            access: &OPEN,
        };
        let outbox = Arc::new(Outbox::default());
        let mut clients = Clients::default();
        let heartbeat =
            async |clients: &mut Clients, groups: &mut Groups, ms, member: &str, epoch, owned: Option<Vec<u32>>| {
                let request = Request::GroupHeartbeat(lotse_proto::GroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: member.into(),
                    member_epoch: epoch,
                    rebalance_timeout_ms: 100,
                    subscribed: (epoch == 0).then(|| vec![7]),
                    owned: owned.map(|p| vec![lotse_proto::TxnResource { resource_id: 7, partitions: p }]),
                });
                let header =
                    RequestHeader { api_key: ApiKey::GROUP_HEARTBEAT, api_version: 0, correlation_id: 1, flags: 0 };
                let view = view_at(ms);
                clients
                    .handle(
                        ClientRequest { outbox: outbox.clone(), header, request, principal: Principal::Anonymous },
                        &view,
                        groups,
                    )
                    .await
                    .unwrap();
                for _ in 0..3 {
                    groups.get_mut(&G).unwrap().process().await.unwrap();
                    clients.after_round(&view, groups).await.unwrap();
                }
                match &drain(&outbox, ApiKey::GROUP_HEARTBEAT).0[..] {
                    [Response::GroupHeartbeat(h)] => h.clone(),
                    other => panic!("expected one heartbeat answer, got {other:?}"),
                }
            };
        let a = heartbeat(&mut clients, &mut groups, 0, "", 0, None).await;
        assert_eq!(a.assignment.as_ref().map(|r| r[0].partitions.len()), Some(2), "alone, it gets both");
        let b = heartbeat(&mut clients, &mut groups, 10, "", 0, None).await;
        assert_eq!(b.assignment.as_ref().map(|r| r.len()), Some(0), "nothing free yet");
        // A learns that one partition is revoked, and keeps reading it.
        let a2 = heartbeat(&mut clients, &mut groups, 20, &a.member_id, a.member_epoch, Some(vec![0, 1])).await;
        assert_eq!(a2.assignment.as_ref().map(|r| r[0].partitions.len()), Some(1));
        // A releases the revoked partition, then tries to commit both.
        let kept = a2.assignment.as_ref().unwrap()[0].partitions.clone();
        heartbeat(&mut clients, &mut groups, 30, &a.member_id, a2.member_epoch, Some(kept.clone())).await;
        let released = 1 - kept[0];
        let commit = |member: &str, partitions: Vec<u32>| {
            Request::CommitOffsets(lotse_proto::CommitOffsetsRequest {
                group_id: "g".into(),
                member_id: member.into(),
                member_epoch: a2.member_epoch,
                resources: vec![lotse_proto::OffsetCommitResource {
                    resource_id: 7,
                    partitions: partitions
                        .into_iter()
                        .map(|p| lotse_proto::OffsetCommitPartition {
                            partition: p,
                            offset: 9,
                            leader_term: 0,
                            metadata: None,
                        })
                        .collect(),
                }],
            })
        };
        let view = view_at(40);
        for (member, parts, expect) in [
            (a.member_id.clone(), vec![kept[0], released], ErrorCode::FENCED_MEMBER_EPOCH),
            ("no-such-member".to_string(), vec![kept[0]], ErrorCode::UNKNOWN_MEMBER_ID),
        ] {
            let header = RequestHeader { api_key: ApiKey::COMMIT_OFFSETS, api_version: 0, correlation_id: 2, flags: 0 };
            let request = commit(&member, parts);
            clients
                .handle(
                    ClientRequest { outbox: outbox.clone(), header, request, principal: Principal::Anonymous },
                    &view,
                    &mut groups,
                )
                .await
                .unwrap();
            for _ in 0..3 {
                groups.get_mut(&G).unwrap().process().await.unwrap();
                clients.after_round(&view, &mut groups).await.unwrap();
            }
            let (r, _) = drain(&outbox, ApiKey::COMMIT_OFFSETS);
            let [Response::CommitOffsets(c)] = &r[..] else { panic!("expected one answer, got {r:?}") };
            let errors: Vec<ErrorCode> = c.resources[0].partitions.iter().map(|p| p.error).collect();
            assert!(errors.contains(&expect), "{member}: {errors:?}");
            assert!(groups[&G].coordinator().groups().offsets.is_empty(), "{member}: a refused commit wrote offsets");
        }
    });
}

/// A queue's limits through the admin requests: a dead-letter queue must be
/// another queue, a queue others dead-letter into cannot be deleted, and the
/// settings are described and changed like the ack timeout.
#[test]
fn dead_letter_settings_are_created_described_changed_and_protect_their_queue() {
    Sim::new(9).run(|rt| async move {
        let cfg = Config::new(1, vec![1]);
        let r = Replica::open(rt.clone(), "m", cfg, Nothing, LogConfig::default()).await.unwrap();
        let mut groups: Groups = BTreeMap::from([(G, r)]);
        for _ in 0..40 {
            let replica = groups.get_mut(&G).unwrap();
            replica.raft().tick();
            replica.process().await.unwrap();
        }
        let addrs = BTreeMap::new();
        let voters = BTreeMap::new();
        let mut clients = Clients::default();
        let ask = async |clients: &mut Clients,
                         groups: &mut Groups,
                         queues: &[QueueConfig],
                         exchanges: &[Exchange],
                         requests: Vec<Request>| {
            let view = ClusterView {
                streams: &[],
                queues,
                exchanges,
                deleted: &NOTHING_DELETED,
                group_session: Duration::from_secs(1),
                group_heartbeat: Duration::from_millis(100),
                admin: AdminView {
                    metadata: Some(G),
                    nodes: &[1],
                    configured_groups: 1,
                    voters: &voters,
                    ..AdminView::default()
                },
                client_addrs: &addrs,
                coordinator: None,
                node: NodeView::default(),
                now: rt.now(),
                unix_ms: 0,
                access: &OPEN,
            };
            let mut outboxes = Vec::new();
            for request in requests {
                let (outbox, key) = (Arc::new(Outbox::default()), request.api_key());
                let header = RequestHeader { api_key: key, api_version: 0, correlation_id: 1, flags: 0 };
                let req = ClientRequest { outbox: outbox.clone(), header, request, principal: Principal::Anonymous };
                clients.handle(req, &view, groups).await.unwrap();
                outboxes.push((outbox, key));
            }
            for _ in 0..5 {
                groups.get_mut(&G).unwrap().process().await.unwrap();
                clients.after_round(&view, groups).await.unwrap();
            }
            outboxes
                .into_iter()
                .map(|(o, key)| match &drain(&o, key).0[..] {
                    [r] => r.clone(),
                    other => panic!("expected one answer, got {other:?}"),
                })
                .collect::<Vec<Response>>()
        };
        let create = |name: &str, dlq: Option<&str>, configs: Vec<(String, String)>| {
            Request::CreateQueue(lotse_proto::CreateQueueRequest {
                tenant: crate::DEFAULT_TENANT.into(),
                name: name.into(),
                priority_levels: 1,
                single_active: false,
                dlq: dlq.map(str::to_string),
                replication: 1,
                configs,
            })
        };
        let configured = |groups: &Groups, names: &[&str]| -> Vec<QueueConfig> {
            let state = groups[&G].metadata();
            names
                .iter()
                .map(|n| {
                    let q = &state.queues[&state.id_of(n).unwrap()];
                    QueueConfig {
                        name: q.name.clone(),
                        resource_id: q.resource_id,
                        group: q.group,
                        ack_timeout: QueueConfig::DEFAULT_ACK_TIMEOUT,
                        limits: Default::default(),
                        dead_letter: None,
                    }
                })
                .collect()
        };
        let admin_error = |r: &Response| match r {
            Response::Admin(a) => a.error,
            other => panic!("{other:?}"),
        };
        let answers = ask(&mut clients, &mut groups, &[], &[], vec![create("dead", None, vec![])]).await;
        let [Response::Create(dead)] = &answers[..] else { panic!("{answers:?}") };
        assert_eq!(dead.error, ErrorCode::NONE);
        let queues = configured(&groups, &["dead"]);
        let settings = vec![("max.deliveries".to_string(), "3".to_string())];
        let answers = ask(
            &mut clients,
            &mut groups,
            &queues,
            &[],
            vec![
                create("bad", Some("nowhere"), vec![]),
                create("zero", None, vec![("max.deliveries".into(), "0".into())]),
                create("work", Some("dead"), settings),
            ],
        )
        .await;
        let [Response::Create(bad), Response::Create(zero), Response::Create(work)] = &answers[..] else {
            panic!("{answers:?}")
        };
        assert_eq!(
            (bad.error, zero.error, work.error),
            (ErrorCode::INVALID_REQUEST, ErrorCode::INVALID_REQUEST, ErrorCode::NONE)
        );
        let limits = groups[&G].metadata().queues[&work.resource_id].limits;
        assert_eq!((limits.max_deliveries, limits.message_ttl_ms, limits.dead_letter), (3, 0, dead.resource_id));
        let queues = configured(&groups, &["dead", "work"]);
        let alter = |set: &[(&str, &str)], delete: &[&str], id| {
            Request::AlterConfigs(lotse_proto::AlterConfigsRequest {
                resource_id: id,
                set: set.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
                delete: delete.iter().map(|k| k.to_string()).collect(),
            })
        };
        let answers = ask(
            &mut clients,
            &mut groups,
            &queues,
            &[],
            vec![
                alter(&[("dead.letter.queue", "work")], &[], work.resource_id),
                alter(&[("dead.letter.queue", "dead")], &[], dead.resource_id),
                alter(&[("message.ttl.ms", "1500")], &[], work.resource_id),
                Request::DeleteResource(lotse_proto::DeleteResourceRequest {
                    resource_id: dead.resource_id,
                    if_empty: false,
                }),
            ],
        )
        .await;
        let described = ask(
            &mut clients,
            &mut groups,
            &queues,
            &[],
            vec![Request::DescribeConfigs(lotse_proto::DescribeConfigsRequest {
                resource_ids: vec![work.resource_id],
            })],
        )
        .await;
        assert_eq!(admin_error(&answers[0]), ErrorCode::INVALID_REQUEST, "its own dead-letter queue");
        assert_eq!(admin_error(&answers[1]), ErrorCode::INVALID_REQUEST, "dead -> work -> dead is a loop");
        assert_eq!(admin_error(&answers[2]), ErrorCode::NONE);
        assert_eq!(admin_error(&answers[3]), ErrorCode::INVALID_REQUEST, "work dead-letters into it");
        let Response::DescribeConfigs(d) = &described[0] else { panic!("{:?}", described[0]) };
        let shown: Vec<(&str, &str)> =
            d.resources[0].configs.iter().map(|c| (c.key.as_str(), c.value.as_str())).collect();
        assert!(shown.contains(&("max.deliveries", "3")) && shown.contains(&("message.ttl.ms", "1500")));
        assert!(shown.contains(&("dead.letter.queue", "dead")), "{shown:?}");
    });
}

/// A bind or an alter admitted before a delete of its queue, and applied
/// after it, changed nothing: it must not be acknowledged. Two admitted in
/// the same round before the delete always land in that order, a history
/// the simulation meets too rarely.
#[test]
fn a_bind_or_alter_overtaken_by_a_delete_is_not_acknowledged() {
    Sim::new(8).run(|rt| async move {
        let cfg = Config::new(1, vec![1]);
        let r = Replica::open(rt.clone(), "m", cfg, Nothing, LogConfig::default()).await.unwrap();
        let mut groups: Groups = BTreeMap::from([(G, r)]);
        for _ in 0..40 {
            let replica = groups.get_mut(&G).unwrap();
            replica.raft().tick();
            replica.process().await.unwrap();
        }
        let addrs = BTreeMap::new();
        let voters = BTreeMap::new();
        let mut clients = Clients::default();
        let ask = async |clients: &mut Clients,
                         groups: &mut Groups,
                         queues: &[QueueConfig],
                         exchanges: &[Exchange],
                         requests: Vec<Request>| {
            let view = ClusterView {
                streams: &[],
                queues,
                exchanges,
                deleted: &NOTHING_DELETED,
                group_session: Duration::from_secs(1),
                group_heartbeat: Duration::from_millis(100),
                admin: AdminView {
                    metadata: Some(G),
                    nodes: &[1],
                    configured_groups: 1,
                    voters: &voters,
                    ..AdminView::default()
                },
                client_addrs: &addrs,
                coordinator: None,
                node: NodeView::default(),
                now: rt.now(),
                unix_ms: 0,
                access: &OPEN,
            };
            let mut outboxes = Vec::new();
            for request in requests {
                let (outbox, key) = (Arc::new(Outbox::default()), request.api_key());
                let header = RequestHeader { api_key: key, api_version: 0, correlation_id: 1, flags: 0 };
                let req = ClientRequest { outbox: outbox.clone(), header, request, principal: Principal::Anonymous };
                clients.handle(req, &view, groups).await.unwrap();
                outboxes.push((outbox, key));
            }
            for _ in 0..5 {
                groups.get_mut(&G).unwrap().process().await.unwrap();
                clients.after_round(&view, groups).await.unwrap();
            }
            outboxes
                .into_iter()
                .map(|(o, key)| match &drain(&o, key).0[..] {
                    [r] => r.clone(),
                    other => panic!("expected one answer, got {other:?}"),
                })
                .collect::<Vec<Response>>()
        };
        let created = ask(
            &mut clients,
            &mut groups,
            &[],
            &[],
            vec![
                Request::CreateQueue(lotse_proto::CreateQueueRequest {
                    tenant: crate::DEFAULT_TENANT.into(),
                    name: "q".into(),
                    priority_levels: 1,
                    single_active: false,
                    dlq: None,
                    replication: 1,
                    configs: vec![],
                }),
                Request::DeclareExchange(lotse_proto::DeclareExchangeRequest {
                    tenant: crate::DEFAULT_TENANT.into(),
                    name: "x".into(),
                    exchange_type: lotse_proto::exchange_type::FANOUT,
                    configs: vec![],
                }),
            ],
        )
        .await;
        let [Response::Create(q), Response::Create(x)] = &created[..] else { panic!("{created:?}") };
        assert_eq!((q.error, x.error), (ErrorCode::NONE, ErrorCode::NONE));
        let state = groups[&G].metadata().clone();
        let dq = &state.queues[&q.resource_id];
        let queues = [QueueConfig {
            name: "q".into(),
            resource_id: q.resource_id,
            group: dq.group,
            ack_timeout: QueueConfig::DEFAULT_ACK_TIMEOUT,
            limits: Default::default(),
            dead_letter: None,
        }];
        let exchanges = [Exchange {
            name: "x".into(),
            resource_id: x.resource_id,
            kind: lotse_route::ExchangeKind::Fanout,
            bindings: vec![],
        }];
        let answers = ask(
            &mut clients,
            &mut groups,
            &queues,
            &exchanges,
            vec![
                Request::Bind(lotse_proto::BindRequest {
                    exchange_id: x.resource_id,
                    queue_id: q.resource_id,
                    binding_key: String::new(),
                    header_match: vec![],
                    match_all: true,
                }),
                Request::AlterConfigs(lotse_proto::AlterConfigsRequest {
                    resource_id: q.resource_id,
                    set: vec![("ack.timeout.ms".into(), "500".into())],
                    delete: vec![],
                }),
                Request::DeleteResource(lotse_proto::DeleteResourceRequest {
                    resource_id: q.resource_id,
                    if_empty: false,
                }),
            ],
        )
        .await;
        let [Response::Outcome(bind), Response::Admin(alter), Response::Admin(delete)] = &answers[..] else {
            panic!("{answers:?}")
        };
        assert_eq!(delete.error, ErrorCode::NONE, "the delete went through");
        assert_eq!(bind.error, ErrorCode::UNKNOWN_RESOURCE, "a bind to a queue deleted before it was acknowledged");
        assert_eq!(alter.error, ErrorCode::UNKNOWN_RESOURCE, "an alter of a deleted queue was acknowledged");
        let state = groups[&G].metadata();
        assert!(state.bindings.values().all(Vec::is_empty) && state.queues.is_empty());

        // A stream's retention: given at creation, changed key by key,
        // refused where it makes no sense, described with the defaults.
        let stream = |configs: Vec<(&str, &str)>| {
            Request::CreateStream(lotse_proto::CreateStreamRequest {
                tenant: crate::DEFAULT_TENANT.into(),
                name: "s".into(),
                partitions: 1,
                replication: 1,
                configs: configs.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
            })
        };
        let created = ask(&mut clients, &mut groups, &[], &[], vec![stream(vec![("retention.ms", "0")])]).await;
        let [Response::Create(c)] = &created[..] else { panic!("{created:?}") };
        assert_eq!(c.error, ErrorCode::INVALID_REQUEST, "no retention of 0 ms");
        let created = ask(&mut clients, &mut groups, &[], &[], vec![stream(vec![("retention.ms", "60000")])]).await;
        let [Response::Create(c)] = &created[..] else { panic!("{created:?}") };
        assert_eq!(c.error, ErrorCode::NONE);
        let alter = |set: Vec<(&str, &str)>, delete: Vec<&str>| {
            Request::AlterConfigs(lotse_proto::AlterConfigsRequest {
                resource_id: c.resource_id,
                set: set.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
                delete: delete.into_iter().map(String::from).collect(),
            })
        };
        let answers = ask(
            &mut clients,
            &mut groups,
            &[],
            &[],
            vec![
                alter(vec![("ack.timeout.ms", "5")], vec![]),
                alter(vec![("retention.bytes", "-2")], vec![]),
                alter(vec![("retention.bytes", "100")], vec!["retention.bytes"]),
                alter(vec![("retention.bytes", "4096")], vec![]),
                alter(vec![], vec!["retention.ms"]),
                Request::DescribeConfigs(lotse_proto::DescribeConfigsRequest { resource_ids: vec![c.resource_id] }),
            ],
        )
        .await;
        let [
            Response::Admin(other_key),
            Response::Admin(negative),
            Response::Admin(twice),
            Response::Admin(bytes),
            Response::Admin(reset),
            Response::DescribeConfigs(described),
        ] = &answers[..]
        else {
            panic!("{answers:?}")
        };
        assert_eq!(
            (other_key.error, negative.error, twice.error),
            (ErrorCode::INVALID_REQUEST, ErrorCode::INVALID_REQUEST, ErrorCode::INVALID_REQUEST)
        );
        assert_eq!((bytes.error, reset.error), (ErrorCode::NONE, ErrorCode::NONE));
        let settings = groups[&G].metadata().streams[&c.resource_id].settings;
        assert_eq!(settings, crate::metadata::StreamSettings { retention_ms: None, retention_bytes: Some(4096) });
        // The describe was answered before the changes applied; ask again.
        let _ = described;
        let answers = ask(
            &mut clients,
            &mut groups,
            &[],
            &[],
            vec![Request::DescribeConfigs(lotse_proto::DescribeConfigsRequest { resource_ids: vec![c.resource_id] })],
        )
        .await;
        let [Response::DescribeConfigs(d)] = &answers[..] else { panic!("{answers:?}") };
        let got: Vec<_> = d.resources[0].configs.iter().map(|e| (e.key.as_str(), e.value.as_str(), e.source)).collect();
        assert_eq!(
            got,
            [
                ("retention.ms", "-1", lotse_proto::config_source::DEFAULT),
                ("retention.bytes", "4096", lotse_proto::config_source::DYNAMIC)
            ]
        );
    });
}

/// A transactional batch is taken only once the coordinator has said that
/// its transaction is open and holds the partition (ADR 0057): until then
/// it waits, written nowhere; refused, it is never written; once a batch is
/// in, the transaction is open here and later batches need no question.
#[test]
fn a_transactional_batch_waits_for_the_coordinator() {
    Sim::new(4).run(|rt| async move {
        let mut r = single(&rt).await;
        lead(&mut r).await;
        let mut groups: Groups = BTreeMap::from([(G, r)]);
        let streams = [StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![G] }];
        let addrs = BTreeMap::new();
        let view = ClusterView {
            streams: &streams,
            queues: &[],
            exchanges: &[],
            deleted: &NOTHING_DELETED,
            group_session: Duration::from_secs(45),
            group_heartbeat: Duration::from_secs(5),
            admin: AdminView::default(),
            client_addrs: &addrs,
            coordinator: None,
            node: NodeView::default(),
            now: rt.now(),
            unix_ms: 0,
            access: &OPEN,
        };
        let outbox = Arc::new(Outbox::default());
        let mut clients = Clients::default();
        let produce = |pid, epoch, seq, correlation_id| ClientRequest {
            outbox: outbox.clone(),
            header: RequestHeader { api_key: ApiKey::PRODUCE, api_version: 0, correlation_id, flags: 0 },
            request: Request::Produce(ProduceRequest {
                txn_id: Some("app".into()),
                timeout_ms: 60_000,
                resources: vec![ProduceResource {
                    resource_id: 7,
                    partitions: vec![ProducePartition { partition: 0, batch: transactional_batch(pid, epoch, seq) }],
                }],
            }),
            principal: Principal::Anonymous,
        };
        let last = groups[&G].raft_ref().last_index();

        // Two batches of producer 5 at epoch 1: one question.
        clients.handle(produce(5, 1, 0, 1), &view, &mut groups).await.unwrap();
        clients.handle(produce(5, 1, 0, 2), &view, &mut groups).await.unwrap();
        let jobs = clients.take_verify_jobs();
        assert_eq!(jobs.len(), 1, "one question per partition, producer and epoch");
        let job = &jobs[0];
        assert_eq!((job.request.txn_id.as_str(), job.request.producer_id, job.request.producer_epoch), ("app", 5, 1));
        assert_eq!((job.request.resource_id, job.request.partition), (7, 0));
        assert!(answers(&outbox).is_empty(), "held, not answered");
        assert_eq!(groups[&G].raft_ref().last_index(), last, "and not written");

        // The coordinator knows of no such transaction: both refused.
        clients.verified(job.key, ErrorCode::INVALID_TXN_STATE, &view, &mut groups);
        let got = answers(&outbox);
        assert_eq!(got.len(), 2);
        for a in got {
            let Response::Produce(p) = a else { panic!("{a:?}") };
            assert_eq!(p.resources[0].partitions[0].error, ErrorCode::INVALID_TXN_STATE);
        }
        assert_eq!(groups[&G].raft_ref().last_index(), last, "nothing written");
        assert!(groups[&G].opens_txn(&transactional_batch(5, 1, 0)).is_some(), "no transaction open");

        // Asked again and confirmed: written.
        clients.handle(produce(5, 1, 0, 3), &view, &mut groups).await.unwrap();
        let jobs = clients.take_verify_jobs();
        assert_eq!(jobs.len(), 1, "asked again after the refusal");
        clients.verified(jobs[0].key, ErrorCode::NONE, &view, &mut groups);
        assert_eq!(groups[&G].raft_ref().last_index(), last + 1, "written once confirmed");
        assert!(groups[&G].opens_txn(&transactional_batch(5, 1, 1)).is_none(), "the transaction is open here now");
        clients.handle(produce(5, 1, 1, 4), &view, &mut groups).await.unwrap();
        assert!(clients.take_verify_jobs().is_empty(), "later batches need no question");
        assert_eq!(groups[&G].raft_ref().last_index(), last + 2);
    });
}

#[test]
fn the_coordinator_confirms_only_an_open_transaction_with_the_partition() {
    use crate::coordinator::{Phase, TxnMeta};
    use coord::verdict;
    let meta = TxnMeta {
        producer_id: 5,
        epoch: 2,
        phase: Phase::Ongoing,
        timeout_ms: 60_000,
        partitions: BTreeSet::from([(7, 0)]),
        offsets: BTreeMap::new(),
    };
    let ask = |producer_id, producer_epoch, partition| lotse_proto::VerifyTxnRequest {
        txn_id: "app".into(),
        producer_id,
        producer_epoch,
        resource_id: 7,
        partition,
    };
    assert_eq!(verdict(Some(&meta), false, &ask(5, 2, 0)), ErrorCode::NONE);
    assert_eq!(verdict(None, false, &ask(5, 2, 0)), ErrorCode::INVALID_TXN_STATE, "an unknown transactional id");
    assert_eq!(verdict(Some(&meta), false, &ask(5, 2, 1)), ErrorCode::INVALID_TXN_STATE, "a partition never added");
    assert_eq!(verdict(Some(&meta), false, &ask(5, 1, 0)), ErrorCode::PRODUCER_FENCED, "an older epoch");
    assert_eq!(verdict(Some(&meta), false, &ask(6, 2, 0)), ErrorCode::PRODUCER_FENCED, "another producer");
    assert_eq!(
        verdict(Some(&meta), true, &ask(5, 2, 0)),
        ErrorCode::CONCURRENT_TRANSACTIONS,
        "while a change is written"
    );
    let ended = TxnMeta { phase: Phase::PrepareCommit, ..meta.clone() };
    assert_eq!(verdict(Some(&ended), false, &ask(5, 2, 0)), ErrorCode::INVALID_TXN_STATE, "ending");
    let empty = TxnMeta { phase: Phase::Empty, partitions: BTreeSet::new(), ..meta };
    assert_eq!(verdict(Some(&empty), false, &ask(5, 2, 0)), ErrorCode::INVALID_TXN_STATE, "none open");
}

/// A compressed batch is decoded once when it is produced (ADR 0058): one
/// whose block does not decode is refused and never written; a good one is
/// written as it came, still compressed.
#[test]
fn a_compressed_batch_that_does_not_decode_is_never_written() {
    Sim::new(5).run(|rt| async move {
        let mut r = single(&rt).await;
        lead(&mut r).await;
        let mut options = BatchOptions::default();
        options.attributes.compression = lotse_record::Compression::Zstd;
        let mut b = BatchBuilder::new(options);
        for _ in 0..50 {
            b.push(0, None, Some(b"compressible, compressible"), &[]);
        }
        let good = b.finish().unwrap();
        // Damage the compressed block, then fix the checksum as a careless
        // producer would have computed it.
        let mut bad = bytes::BytesMut::from(&good[..]);
        let mid = lotse_record::HEADER_LEN + (bad.len() - lotse_record::HEADER_LEN) / 2;
        bad[mid] ^= 0xff;
        bad[lotse_record::HEADER_LEN] ^= 0xff;
        let crc = crc32c::crc32c(&bad[25..]);
        bad[21..25].copy_from_slice(&crc.to_le_bytes());
        let bad = bad.freeze();
        assert!(Batch::parse(&bad).is_ok(), "header and checksum are fine");
        let last = r.raft_ref().last_index();
        assert!(matches!(r.propose(bad), Err(ProposeError::Invalid(_))), "refused");
        assert_eq!(r.raft_ref().last_index(), last, "and not written");
        let p = r.propose(good.clone()).expect("a good compressed batch");
        r.process().await.unwrap();
        let stored = r.log().read(p.first, 1 << 20).await.unwrap();
        let (batch, _) = Batch::parse(&stored).unwrap();
        assert_eq!(batch.attributes().compression, lotse_record::Compression::Zstd, "stored compressed");
        assert_eq!(batch.records().unwrap().len(), 50);
    });
}
