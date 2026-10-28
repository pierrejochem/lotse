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
        let mut nodes = BTreeMap::new();
        for id in 1..=5 {
            let cfg = Config { pre_vote: false, ..Config::new(id, vec![1, 2, 3, 4, 5]) };
            let disk = rt.node(&format!("n{id}"));
            let r = Replica::open(disk, "g", cfg, Nothing, LogConfig::default()).await.unwrap();
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
        let view = ClusterView { streams: &streams, client_addrs: &addrs, now: rt.now() };
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
