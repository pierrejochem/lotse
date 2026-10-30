//! Raft for Lotse, without I/O.
//!
//! [`RaftGroup`] is one Raft state machine: it never touches a disk, a
//! socket or a clock. The driver (`lotse-server`, or a simulation) feeds it
//! messages with [`RaftGroup::step`] and time with [`RaftGroup::tick`], then
//! takes a [`Ready`]: what to persist, what to apply and what to send. After
//! persisting it calls [`RaftGroup::advance`].
//!
//! # Indices are record offsets
//!
//! The partition log is the Raft log. An entry is one record batch and covers
//! the indices `first..=last`, one per record; index = offset + 1, so 0 can
//! mean "nothing". Terms are kept as runs ([`TermRuns`]); record bytes stay
//! in the driver's log. Messages to followers name the index to send from
//! ([`Send::fill_from`]); the driver reads whole batches from its log and puts
//! them in the message, so replication never copies records through Raft.
//!
//! # Snapshots
//!
//! The driver snapshots its applied state when it likes and calls
//! [`RaftGroup::compact`]; the core forgets the log up to there. A follower
//! too far behind gets a [`Message::Snapshot`], whose data the driver fills
//! in ([`Send::attach_snapshot`]) and installs from [`Ready::snapshot`]. The
//! core only knows a snapshot's [`SnapshotMeta`].
//!
//! # Membership
//!
//! Members change through config entries in the log ([`EntryKind::Config`]),
//! in force as soon as they are appended. New nodes join as learners, which
//! receive the log but do not vote. A change of voters goes through a joint
//! configuration that needs a majority of the old and of the new voters for
//! every election and commit ([`Membership`]); once it commits, the leader
//! appends the final configuration on its own. See
//! [`RaftGroup::propose_membership`].
//!
//! # What is in
//!
//! Leader election with optional pre-vote, check-quorum and leader stickiness (a node
//! that heard from a leader recently ignores vote requests), log replication with one
//! in-flight append per follower and conflict-term backtracking, a no-op
//! entry when a leader takes over so earlier entries commit, the commit rule
//! that only counts entries of the leader's own term, snapshots, learners and
//! joint consensus.
//!
//! # Leader transfer
//!
//! [`RaftGroup::transfer_leadership`] hands leadership to a voter: the leader
//! stops taking proposals, brings the target up to date and tells it to
//! campaign at once ([`Message::TimeoutNow`]); its vote requests pass leader
//! stickiness. If nothing happens within an election timeout, the leader
//! carries on.
//!
//! # Linearizable reads
//!
//! [`RaftGroup::read_index`] confirms that this node still leads before a
//! read is served: every append and heartbeat carries a round number
//! (`seq`) that followers echo, and a read waits for a quorum to answer a
//! round sent after it arrived. Reads arriving between two [`Ready`]s share
//! one round. A new leader first commits an entry of its own term. The
//! driver serves each [`ReadState`] once it has applied up to its index.
//!
//! With [`Config::lease_ticks`], the leader instead serves reads at once
//! while a quorum answered a round sent less than `lease_ticks` ago. That is
//! only safe if no clock runs much faster than another: followers ignore
//! votes for an election timeout after they last heard from the leader, and
//! after a restart, so the lease must end well before that.

#![warn(missing_docs)]

mod membership;
mod multi;
mod terms;
pub mod testing;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

pub use membership::Membership;
pub use multi::{GroupReady, MultiRaft, MultiReady};
pub use terms::TermRuns;

/// A node of the cluster.
pub type NodeId = u64;

/// What an entry carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// A record batch from a producer.
    Normal,
    /// The empty entry a new leader appends so earlier entries can commit.
    /// The driver stores it as a control batch that readers skip.
    Noop,
    /// A new [`Membership`], encoded in `data`. Also a control batch.
    Config,
}

/// One Raft entry: a record batch covering `first..=last`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// First index.
    pub first: u64,
    /// Last index.
    pub last: u64,
    /// Term it was proposed in.
    pub term: u64,
    /// Kind.
    pub kind: EntryKind,
    /// The encoded batch; empty for [`EntryKind::Noop`].
    pub data: Bytes,
}

/// State that must be durable before any message of the same [`Ready`] is
/// sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HardState {
    /// Current term.
    pub term: u64,
    /// Who this node voted for in `term`.
    pub vote: Option<NodeId>,
    /// Highest index known to be committed.
    pub commit: u64,
}

/// What the core knows of a snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// Last index the snapshot contains (the last index of an entry).
    pub index: u64,
    /// Term of `index`.
    pub term: u64,
    /// Membership in force at `index`.
    pub membership: Membership,
}

/// A snapshot to install: replace the state machine with `data` and drop the
/// whole log; it continues after `meta.index`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Where it stands.
    pub meta: SnapshotMeta,
    /// The driver's encoded state.
    pub data: Bytes,
}

/// Outcome of an append, as the follower reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendResult {
    /// The follower's log now matches the leader's up to `last`.
    Accepted {
        /// Last index known to match.
        last: u64,
    },
    /// The follower does not have `prev_index` with `prev_term`.
    Rejected {
        /// The `prev_index` of the rejected append.
        prev_index: u64,
        /// The follower's last index.
        last_index: u64,
        /// The follower's term at `prev_index` and the first index of that
        /// term, so the leader can skip a whole term at once.
        conflict: Option<(u64, u64)>,
    },
}

/// A Raft message between nodes of one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Asks for a vote. With `pre`, asks whether the node *would* vote,
    /// without anyone changing term.
    Vote {
        /// Term the candidate campaigns in.
        term: u64,
        /// Candidate's last index.
        last_index: u64,
        /// Candidate's last term.
        last_term: u64,
        /// Pre-vote.
        pre: bool,
        /// Sent on the leader's request ([`Message::TimeoutNow`]): passes
        /// leader stickiness.
        transfer: bool,
    },
    /// Answer to [`Message::Vote`].
    VoteReply {
        /// For a granted pre-vote, the term asked for; otherwise the voter's term.
        term: u64,
        /// Granted.
        granted: bool,
        /// Answers a pre-vote.
        pre: bool,
    },
    /// Entries to append, or an empty heartbeat.
    Append {
        /// Leader's term.
        term: u64,
        /// Index right before `entries`.
        prev_index: u64,
        /// Term at `prev_index`.
        prev_term: u64,
        /// Entries starting at `prev_index + 1`. The core leaves this empty
        /// and sets [`Send::fill_from`]; the driver fills it from its log.
        entries: Vec<Entry>,
        /// Leader's commit index.
        commit: u64,
        /// Leader's round, echoed in the reply.
        seq: u64,
    },
    /// Answer to [`Message::Append`] and [`Message::Snapshot`].
    AppendReply {
        /// Follower's term.
        term: u64,
        /// Outcome.
        result: AppendResult,
        /// The round of the message answered.
        seq: u64,
    },
    /// The leader's snapshot, for a follower whose next entry the leader
    /// has compacted away. Answered with an [`Message::AppendReply`].
    Snapshot {
        /// Leader's term.
        term: u64,
        /// Where the snapshot stands. The driver may replace it with a newer
        /// snapshot's, together with `data`.
        meta: SnapshotMeta,
        /// The core leaves this empty and sets [`Send::attach_snapshot`].
        data: Bytes,
        /// Leader's round, echoed in the reply.
        seq: u64,
    },
    /// The leader hands over: campaign now, without pre-vote.
    TimeoutNow {
        /// Leader's term.
        term: u64,
    },
}

impl Message {
    /// Term carried by the message.
    pub fn term(&self) -> u64 {
        match self {
            Message::Vote { term, .. }
            | Message::VoteReply { term, .. }
            | Message::Append { term, .. }
            | Message::AppendReply { term, .. }
            | Message::Snapshot { term, .. }
            | Message::TimeoutNow { term } => *term,
        }
    }
}

/// A message to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Send {
    /// Destination.
    pub to: NodeId,
    /// The message.
    pub msg: Message,
    /// For appends: fill `entries` with whole entries starting at this index
    /// (always an entry boundary), as many as fit the driver's size limit,
    /// but at least one. If the driver has compacted that far meanwhile, it
    /// sends the append empty.
    pub fill_from: Option<u64>,
    /// For snapshots: put the driver's latest snapshot (meta and data) in.
    pub attach_snapshot: bool,
}

/// Everything the driver has to do, in this order: install `snapshot`,
/// truncate the log at `truncate_from`, append `entries`, persist
/// `hard_state`, apply the `committed` range, then send `messages`.
///
/// The hard state comes after the log on purpose: its commit index refers to
/// the entries of the same ready. Persisted before them, a crash could leave
/// it pointing into a log that still holds a conflicting suffix. (A driver
/// that writes the log and the hard state in one atomic write may ignore
/// this.)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ready {
    /// New hard state, if it changed.
    pub hard_state: Option<HardState>,
    /// Replace the state machine and the whole log with this.
    pub snapshot: Option<Snapshot>,
    /// Remove every index from here on before appending.
    pub truncate_from: Option<u64>,
    /// Entries to append, in order.
    pub entries: Vec<Entry>,
    /// Newly committed indices `(first, last)`, to apply in order.
    pub committed: Option<(u64, u64)>,
    /// Confirmed reads: serve each once applied up to its index.
    pub reads: Vec<ReadState>,
    /// Reads that will not be confirmed (this node stopped leading): the
    /// client should retry with the leader.
    pub reads_aborted: Vec<u64>,
    /// Messages to send once the above is durable.
    pub messages: Vec<Send>,
}

/// A confirmed linearizable read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadState {
    /// The driver's id for the read, from [`RaftGroup::read_index`].
    pub ctx: u64,
    /// Serve the read once the state machine has applied this index.
    pub index: u64,
}

impl Ready {
    /// `true` if there is nothing to do.
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.snapshot.is_none()
            && self.truncate_from.is_none()
            && self.entries.is_empty()
            && self.committed.is_none()
            && self.reads.is_empty()
            && self.reads_aborted.is_empty()
            && self.messages.is_empty()
    }
}

/// Timing and behaviour.
#[derive(Debug, Clone)]
pub struct Config {
    /// This node.
    pub id: NodeId,
    /// Voters of a brand-new group, this node included. A node that joins
    /// an existing group starts with none and learns the membership from
    /// the leader. Ignored once the log or a snapshot holds a membership.
    pub voters: Vec<NodeId>,
    /// Minimum ticks without a leader before an election. The actual
    /// timeout is random in `election_ticks..2 * election_ticks`.
    pub election_ticks: u32,
    /// Ticks between heartbeats.
    pub heartbeat_ticks: u32,
    /// Ask with a pre-vote before raising the term.
    ///
    /// Check-quorum is always on and not configurable: a leader steps down
    /// when it has not heard from a quorum for an election timeout, and
    /// nodes ignore vote requests while they hear from a leader. Without
    /// that stickiness, a node that was dropped from the voters but has not
    /// learnt so keeps raising the term and unseats every leader, pre-vote
    /// or not.
    pub pre_vote: bool,
    /// Serve reads from a leader lease of this many ticks instead of a
    /// heartbeat round; `None` (the default) for ReadIndex only. Must be
    /// below `election_ticks` by more than the clock drift between nodes.
    pub lease_ticks: Option<u32>,
    /// Seed for the randomised election timeout.
    pub seed: u64,
}

impl Config {
    /// Defaults for node `id` among `voters`: 10-tick elections,
    /// heartbeats every 2 ticks, pre-vote on.
    pub fn new(id: NodeId, voters: Vec<NodeId>) -> Self {
        Config { id, voters, election_ticks: 10, heartbeat_ticks: 2, pre_vote: true, lease_ticks: None, seed: id }
    }
}

/// What a node knows after a restart.
#[derive(Debug, Clone, Default)]
pub struct Restored {
    /// Last persisted hard state.
    pub hard_state: HardState,
    /// Terms of the persisted log (starting after the snapshot, if any).
    pub log: TermRuns,
    /// Highest index the driver has applied.
    pub applied: u64,
    /// The snapshot the log starts after.
    pub snapshot: Option<SnapshotMeta>,
    /// Config entries in the persisted log after the snapshot: `(last index
    /// of the entry, membership)`, in order.
    pub configs: Vec<(u64, Membership)>,
}

/// Role of a node in its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Follows a leader, or waits for one.
    Follower,
    /// Asks whether others would vote for it.
    PreCandidate,
    /// Asks for votes in a new term.
    Candidate,
    /// Leads the group.
    Leader,
}

/// Returned by [`RaftGroup::propose`] on a node that does not lead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotLeader {
    /// The leader this node knows of, if any.
    pub leader: Option<NodeId>,
}

/// Why [`RaftGroup::transfer_leadership`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferError {
    /// Only the leader hands over.
    NotLeader(NotLeader),
    /// The target is not a voter, or a membership change is joint.
    NotVoter,
}

/// Why [`RaftGroup::propose_membership`] refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeError {
    /// Only the leader changes members.
    NotLeader(NotLeader),
    /// A change is still in progress: wait until it commits.
    InProgress,
    /// A node to become voter is not a learner that has caught up.
    NotCaughtUp(NodeId),
    /// No voters, or a node both voter and learner.
    Invalid,
}

#[derive(Debug, Clone)]
struct Progress {
    matched: u64,
    next: u64,
    inflight: bool,
    inflight_ticks: u32,
    active: bool,
    /// Highest round it answered.
    acked: u64,
}

#[derive(Debug, Clone, Copy)]
struct Transfer {
    to: NodeId,
    elapsed: u32,
}

#[derive(Debug, Clone)]
enum State {
    Follower,
    PreCandidate { granted: BTreeSet<NodeId>, rejected: BTreeSet<NodeId> },
    Candidate { granted: BTreeSet<NodeId>, rejected: BTreeSet<NodeId> },
    Leader { progress: BTreeMap<NodeId, Progress> },
}

/// One Raft group on one node. See the crate docs.
#[derive(Debug)]
pub struct RaftGroup {
    cfg: Config,
    /// Membership at the snapshot (or the bootstrap voters).
    base_membership: Membership,
    /// Config entries after the snapshot: `(last index, membership)`.
    configs: Vec<(u64, Membership)>,
    term: u64,
    vote: Option<NodeId>,
    commit: u64,
    applied: u64,
    leader: Option<NodeId>,
    state: State,
    log: TermRuns,
    persisted: u64,
    ready_last: u64,
    election_elapsed: u32,
    heartbeat_elapsed: u32,
    timeout: u32,
    rng: u64,
    hs_dirty: bool,
    pending_snapshot: Option<Snapshot>,
    pending_truncate: Option<u64>,
    pending_entries: Vec<Entry>,
    outbox: Vec<Send>,
    /// Ticks since this node started.
    ticks: u64,
    /// With leases: ticks left during which a just-started node ignores
    /// vote requests (it may have forgotten a leader whose lease runs).
    quiet: u32,
    /// Current round; the leader stamps every append with it.
    seq: u64,
    /// Rounds of this leadership and the tick each was started.
    rounds: std::collections::VecDeque<(u64, u64)>,
    /// A round has to go out for waiting reads.
    need_round: bool,
    /// Reads waiting for this leader's first commit.
    reads_waiting: Vec<u64>,
    /// Reads waiting for a quorum to answer `seq`: (ctx, index, seq).
    reads_pending: Vec<(u64, u64, u64)>,
    reads_ready: Vec<ReadState>,
    reads_aborted: Vec<u64>,
    transfer: Option<Transfer>,
    /// This leader tried a transfer in its term. A delayed `TimeoutNow`
    /// may still make the target campaign past stickiness, so the lease is
    /// off for the rest of the term (reads go through ReadIndex).
    transfer_tried: bool,
}

impl RaftGroup {
    /// A node of a group, from what it persisted (a fresh node uses
    /// `Restored::default()`).
    pub fn new(cfg: Config, restored: Restored) -> Self {
        assert!(cfg.election_ticks > cfg.heartbeat_ticks, "elections must take longer than heartbeats");
        assert!(cfg.lease_ticks.is_none_or(|l| l < cfg.election_ticks), "a lease must end before elections can start");
        let last = restored.log.last_index();
        let snap = restored.log.snapshot_index();
        let base_membership = match restored.snapshot {
            Some(meta) => {
                assert_eq!((meta.index, meta.term), (snap, restored.log.snapshot_term()), "log starts at the snapshot");
                meta.membership
            }
            None => Membership::of_voters(cfg.voters.iter().copied()),
        };
        let commit = restored.hard_state.commit.max(snap).min(last);
        // The log is written before the hard state, so a crash can leave
        // entries of a term the hard state never recorded. The term never
        // stays below them (no vote in that term was ever sent: messages go
        // out only after the hard state is durable).
        let (term, vote) = if restored.log.last_term() > restored.hard_state.term {
            (restored.log.last_term(), None)
        } else {
            (restored.hard_state.term, restored.hard_state.vote)
        };
        let mut g = RaftGroup {
            rng: cfg.seed ^ 0x9E37_79B9_7F4A_7C15,
            base_membership,
            configs: restored.configs,
            term,
            vote,
            commit,
            applied: restored.applied.max(snap).min(commit),
            leader: None,
            state: State::Follower,
            log: restored.log,
            persisted: last,
            ready_last: last,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            timeout: 0,
            hs_dirty: false,
            pending_snapshot: None,
            pending_truncate: None,
            pending_entries: Vec::new(),
            outbox: Vec::new(),
            ticks: 0,
            quiet: if cfg.lease_ticks.is_some() { cfg.election_ticks } else { 0 },
            seq: 0,
            rounds: std::collections::VecDeque::new(),
            need_round: false,
            reads_waiting: Vec::new(),
            reads_pending: Vec::new(),
            reads_ready: Vec::new(),
            reads_aborted: Vec::new(),
            transfer: None,
            transfer_tried: false,
            cfg,
        };
        g.reset_timeout();
        g
    }

    /// This node.
    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    /// Current term.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Current role.
    pub fn role(&self) -> Role {
        match self.state {
            State::Follower => Role::Follower,
            State::PreCandidate { .. } => Role::PreCandidate,
            State::Candidate { .. } => Role::Candidate,
            State::Leader { .. } => Role::Leader,
        }
    }
    /// The leader this node knows of.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    /// Highest committed index known here.
    pub fn commit(&self) -> u64 {
        self.commit
    }
    /// Last index in the log, persisted or not.
    pub fn last_index(&self) -> u64 {
        self.log.last_index()
    }
    /// Terms of the log.
    pub fn log(&self) -> &TermRuns {
        &self.log
    }
    /// On a leader: the highest index each member is known to hold, this
    /// node included; `None` elsewhere. For metrics only.
    pub fn matched(&self) -> Option<BTreeMap<NodeId, u64>> {
        let State::Leader { progress } = &self.state else { return None };
        let mut m: BTreeMap<NodeId, u64> = progress.iter().map(|(id, p)| (*id, p.matched)).collect();
        m.insert(self.cfg.id, self.log.last_index());
        Some(m)
    }
    /// The membership in force: the latest in the log, committed or not.
    pub fn membership(&self) -> &Membership {
        self.configs.last().map_or(&self.base_membership, |c| &c.1)
    }
    /// Last index of the latest config entry, or the snapshot index if
    /// the membership comes from there.
    fn membership_index(&self) -> u64 {
        self.configs.last().map_or(self.log.snapshot_index(), |c| c.0)
    }

    fn next_rand(&mut self) -> u64 {
        // xorshift64*: deterministic from the seed.
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn reset_timeout(&mut self) {
        let e = u64::from(self.cfg.election_ticks);
        self.timeout = (e + self.next_rand() % e) as u32;
    }

    fn send(&mut self, to: NodeId, msg: Message) {
        self.outbox.push(Send { to, msg, fill_from: None, attach_snapshot: false });
    }

    /// Voters other than this node.
    fn other_voters(&self) -> Vec<NodeId> {
        self.membership().all_voters().into_iter().filter(|v| *v != self.cfg.id).collect()
    }

    /// Nodes the leader replicates to.
    fn followers(&self) -> Vec<NodeId> {
        match &self.state {
            State::Leader { progress } => progress.keys().copied().collect(),
            _ => Vec::new(),
        }
    }

    // ----- time ---------------------------------------------------------

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        self.ticks += 1;
        self.quiet = self.quiet.saturating_sub(1);
        if matches!(self.state, State::Leader { .. }) {
            self.tick_leader();
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.timeout && self.can_campaign() {
                self.election_elapsed = 0;
                if self.cfg.pre_vote {
                    self.become_pre_candidate();
                } else {
                    self.campaign();
                }
            }
        }
    }

    fn tick_leader(&mut self) {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;
        let election = self.cfg.election_ticks;
        if let Some(t) = &mut self.transfer {
            t.elapsed += 1;
            if t.elapsed >= election {
                self.transfer = None; // the target did not take over: carry on
            }
        }
        if let State::Leader { progress } = &mut self.state {
            for p in progress.values_mut() {
                if p.inflight {
                    p.inflight_ticks += 1;
                    // The append or its reply got lost: try again.
                    if p.inflight_ticks >= election {
                        p.inflight = false;
                    }
                }
            }
        }
        if self.election_elapsed >= election {
            self.election_elapsed = 0;
            let me = self.cfg.id;
            let membership = self.membership().clone();
            if let State::Leader { progress } = &mut self.state {
                let mut active: BTreeSet<NodeId> =
                    progress.iter().filter(|(_, p)| p.active).map(|(id, _)| *id).collect();
                active.insert(me);
                progress.values_mut().for_each(|p| p.active = false);
                if !membership.has_quorum(&active) {
                    let term = self.term;
                    self.become_follower(term, None);
                    return;
                }
            }
        }
        if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
            self.heartbeat_elapsed = 0;
            self.start_round();
        }
    }

    /// Starts a round: a heartbeat (or append) with a new `seq` to everyone.
    fn start_round(&mut self) {
        self.seq += 1;
        self.rounds.push_back((self.seq, self.ticks));
        self.need_round = false;
        for f in self.followers() {
            self.replicate_or_heartbeat(f);
        }
        self.check_reads();
    }

    /// A voter campaigns. So does a node that the newest, still uncommitted
    /// configuration drops but the one before counts: it may hold the
    /// longest log, and without it nobody might be able to win and commit
    /// the change. It counts votes by the newest configuration and steps down
    /// once that commits. After that, a removed node stays quiet.
    fn can_campaign(&self) -> bool {
        let me = self.cfg.id;
        if self.membership().is_voter(me) {
            return true;
        }
        if self.membership_index() <= self.commit {
            return false;
        }
        let previous = match self.configs.len() {
            0 => return false,
            1 => &self.base_membership,
            n => &self.configs[n - 2].1,
        };
        previous.is_voter(me)
    }

    // ----- role changes -------------------------------------------------

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        if matches!(self.state, State::Leader { .. }) {
            self.reads_aborted.append(&mut self.reads_waiting);
            self.reads_aborted.extend(self.reads_pending.drain(..).map(|r| r.0));
            self.rounds.clear();
            self.need_round = false;
            self.transfer = None;
        }
        if term > self.term {
            self.term = term;
            self.vote = None;
            self.hs_dirty = true;
        }
        self.state = State::Follower;
        self.leader = leader;
        self.election_elapsed = 0;
        self.reset_timeout();
    }

    fn become_pre_candidate(&mut self) {
        let me = BTreeSet::from([self.cfg.id]);
        if self.membership().has_quorum(&me) {
            self.campaign();
            return;
        }
        self.state = State::PreCandidate { granted: me, rejected: BTreeSet::new() };
        self.leader = None;
        self.reset_timeout();
        let (last_index, last_term) = (self.log.last_index(), self.log.last_term());
        let term = self.term + 1;
        for v in self.other_voters() {
            self.send(v, Message::Vote { term, last_index, last_term, pre: true, transfer: false });
        }
    }

    fn campaign(&mut self) {
        self.campaign_with(false);
    }

    fn campaign_with(&mut self, transfer: bool) {
        self.term += 1;
        self.vote = Some(self.cfg.id);
        self.hs_dirty = true;
        self.leader = None;
        let me = BTreeSet::from([self.cfg.id]);
        let alone = self.membership().has_quorum(&me);
        self.state = State::Candidate { granted: me, rejected: BTreeSet::new() };
        self.election_elapsed = 0;
        self.reset_timeout();
        if alone {
            self.become_leader();
            return;
        }
        let (last_index, last_term, term) = (self.log.last_index(), self.log.last_term(), self.term);
        for v in self.other_voters() {
            self.send(v, Message::Vote { term, last_index, last_term, pre: false, transfer });
        }
    }

    fn become_leader(&mut self) {
        self.state = State::Leader { progress: BTreeMap::new() };
        // Followers are probably close behind: start at the end and back off.
        self.sync_progress(self.log.last_index() + 1);
        self.leader = Some(self.cfg.id);
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.rounds.clear();
        self.transfer_tried = false;
        // Entries of earlier terms only commit through an entry of this term.
        self.append_own(1, EntryKind::Noop, Bytes::new());
        self.start_round();
        self.maybe_commit();
    }

    /// Makes the leader's progress cover exactly the members other than
    /// itself; new ones start at `next`.
    fn sync_progress(&mut self, next: u64) {
        let me = self.cfg.id;
        let members = self.membership().all();
        if let State::Leader { progress } = &mut self.state {
            progress.retain(|id, _| members.contains(id));
            for id in members.into_iter().filter(|id| *id != me) {
                progress.entry(id).or_insert(Progress {
                    matched: 0,
                    next,
                    inflight: false,
                    inflight_ticks: 0,
                    active: true,
                    acked: 0,
                });
            }
        }
    }

    // ----- proposals ----------------------------------------------------

    /// Appends a batch of `records` records as the leader. Returns the
    /// indices it occupies; the driver stamps the batch with
    /// `base_offset = first - 1` and the current term.
    pub fn propose(&mut self, records: u64, data: Bytes) -> Result<(u64, u64), NotLeader> {
        self.check_leading()?;
        let range = self.append_own(records.max(1), EntryKind::Normal, data);
        for f in self.followers() {
            self.replicate(f);
        }
        Ok(range)
    }

    /// Changes the members to `voters` and `learners`. Learners can be
    /// added and removed freely; a new voter must be a learner that has
    /// caught up with the commit index. A change of voters goes through a
    /// joint configuration, and the leader finishes it on its own once that
    /// commits. One change at a time.
    pub fn propose_membership(
        &mut self,
        voters: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
    ) -> Result<(u64, u64), ChangeError> {
        let tag = self.membership().tag;
        self.propose_membership_tagged(voters, learners, tag)
    }

    /// [`RaftGroup::propose_membership`] with the driver's `tag`
    /// ([`Membership::tag`]).
    pub fn propose_membership_tagged(
        &mut self,
        voters: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
        tag: u64,
    ) -> Result<(u64, u64), ChangeError> {
        self.check_leading().map_err(ChangeError::NotLeader)?;
        let State::Leader { progress } = &self.state else { unreachable!() };
        if voters.is_empty() || !voters.is_disjoint(&learners) {
            return Err(ChangeError::Invalid);
        }
        let current = self.membership();
        if current.is_joint() || self.membership_index() > self.commit {
            return Err(ChangeError::InProgress);
        }
        for v in voters.difference(&current.voters) {
            let caught_up = current.learners.contains(v) && progress.get(v).is_some_and(|p| p.matched >= self.commit);
            if !caught_up {
                return Err(ChangeError::NotCaughtUp(*v));
            }
        }
        let next = if voters == current.voters {
            Membership { voters, outgoing: BTreeSet::new(), learners, tag }
        } else {
            let outgoing = current.voters.clone();
            let learners = learners.difference(&outgoing).copied().collect();
            Membership { voters, outgoing, learners, tag }
        };
        let range = self.append_config(next);
        for f in self.followers() {
            self.replicate(f);
        }
        Ok(range)
    }

    /// `Ok` on a leader that is not handing over; otherwise where to go.
    fn check_leading(&self) -> Result<(), NotLeader> {
        match (&self.state, self.transfer) {
            (State::Leader { .. }, None) => Ok(()),
            (State::Leader { .. }, Some(t)) => Err(NotLeader { leader: Some(t.to) }),
            _ => Err(NotLeader { leader: self.leader }),
        }
    }

    // ----- leader transfer ----------------------------------------------

    /// Hands leadership to voter `to`. Proposals are refused (with `to` as
    /// the leader to try) until it takes over or an election timeout passes.
    pub fn transfer_leadership(&mut self, to: NodeId) -> Result<(), TransferError> {
        self.check_leading().map_err(TransferError::NotLeader)?;
        if to == self.cfg.id {
            return Ok(());
        }
        let m = self.membership();
        if m.is_joint() || !m.voters.contains(&to) {
            return Err(TransferError::NotVoter);
        }
        self.transfer = Some(Transfer { to, elapsed: 0 });
        self.transfer_tried = true;
        // Probe the target afresh: an append to it may be lost in flight.
        if let State::Leader { progress } = &mut self.state
            && let Some(p) = progress.get_mut(&to)
        {
            p.inflight = false;
        }
        self.maybe_timeout_now(to);
        Ok(())
    }

    /// Tells the transfer target to campaign once it has the whole log.
    fn maybe_timeout_now(&mut self, f: NodeId) {
        if self.transfer.is_none_or(|t| t.to != f) {
            return;
        }
        let last = self.log.last_index();
        let State::Leader { progress } = &self.state else { return };
        if progress.get(&f).is_some_and(|p| p.matched == last) {
            let term = self.term;
            self.send(f, Message::TimeoutNow { term });
        } else {
            self.replicate(f);
        }
    }

    // ----- linearizable reads -------------------------------------------

    /// Asks to confirm a linearizable read with the driver's id `ctx`. It
    /// shows up in [`Ready::reads`] (or [`Ready::reads_aborted`]).
    pub fn read_index(&mut self, ctx: u64) -> Result<(), NotLeader> {
        self.check_leading()?;
        if self.log.term_at(self.commit) != Some(self.term) {
            // A new leader's commit index may lag: wait for its own entry.
            self.reads_waiting.push(ctx);
            return Ok(());
        }
        self.register_read(ctx);
        Ok(())
    }

    fn register_read(&mut self, ctx: u64) {
        let index = self.commit;
        if self.lease_valid() {
            self.reads_ready.push(ReadState { ctx, index });
        } else {
            self.reads_pending.push((ctx, index, self.seq + 1));
            self.need_round = true;
        }
    }

    /// Highest round a quorum (this node included) has answered.
    fn quorum_seq(&self) -> u64 {
        let State::Leader { progress } = &self.state else { return 0 };
        let (me, seq) = (self.cfg.id, self.seq);
        self.membership().committed(|v| if v == me { seq } else { progress.get(&v).map_or(0, |p| p.acked) })
    }

    fn lease_valid(&self) -> bool {
        let Some(lease) = self.cfg.lease_ticks else { return false };
        if self.transfer_tried {
            return false;
        }
        let q = self.quorum_seq();
        // The round's start tick: a quorum heard from us after it.
        self.rounds.iter().rev().find(|r| r.0 <= q).is_some_and(|r| self.ticks < r.1 + u64::from(lease))
    }

    fn check_reads(&mut self) {
        let q = self.quorum_seq();
        let mut i = 0;
        while i < self.reads_pending.len() {
            if self.reads_pending[i].2 <= q {
                let (ctx, index, _) = self.reads_pending.remove(i);
                self.reads_ready.push(ReadState { ctx, index });
            } else {
                i += 1;
            }
        }
        // Rounds older than what a quorum answered are no longer needed.
        while self.rounds.len() > 1 && self.rounds.get(1).is_some_and(|r| r.0 <= q) {
            self.rounds.pop_front();
        }
    }

    fn append_config(&mut self, m: Membership) -> (u64, u64) {
        let range = self.append_own(1, EntryKind::Config, m.encode());
        self.configs.push((range.1, m));
        // A node that just joined usually has nothing: start from the
        // beginning (the snapshot, or index 1).
        self.sync_progress(1);
        range
    }

    fn append_own(&mut self, records: u64, kind: EntryKind, data: Bytes) -> (u64, u64) {
        let first = self.log.last_index() + 1;
        let last = first + records - 1;
        self.log.append(first, last, self.term);
        self.pending_entries.push(Entry { first, last, term: self.term, kind, data });
        (first, last)
    }

    // ----- snapshots ----------------------------------------------------

    /// The driver has a snapshot of its state up to `index` (the last index
    /// of an applied entry): forget the log up to there.
    pub fn compact(&mut self, index: u64) {
        assert!(index <= self.applied, "compact only what is applied");
        if index <= self.log.snapshot_index() {
            return;
        }
        let membership = self.membership_at(index);
        self.log.compact(index);
        self.configs.retain(|c| c.0 > index);
        self.base_membership = membership;
    }

    /// Membership in force at `index`.
    fn membership_at(&self, index: u64) -> Membership {
        self.configs.iter().rev().find(|c| c.0 <= index).map_or_else(|| self.base_membership.clone(), |c| c.1.clone())
    }

    /// What a snapshot at `index` would say: its term and the membership
    /// in force there. `None` if the log no longer (or not yet) knows the
    /// index.
    pub fn meta_at(&self, index: u64) -> Option<SnapshotMeta> {
        if index < self.log.snapshot_index() || index > self.log.last_index() {
            return None;
        }
        let term = self.log.term_at(index)?;
        Some(SnapshotMeta { index, term, membership: self.membership_at(index) })
    }

    /// Where this node's snapshot stands.
    pub fn snapshot_meta(&self) -> SnapshotMeta {
        SnapshotMeta {
            index: self.log.snapshot_index(),
            term: self.log.snapshot_term(),
            membership: self.base_membership.clone(),
        }
    }

    // ----- replication (leader side) -------------------------------------

    fn replicate(&mut self, f: NodeId) {
        let last = self.log.last_index();
        let (term, commit, seq) = (self.term, self.commit, self.seq);
        let snapshot = self.snapshot_meta();
        let State::Leader { progress } = &mut self.state else { return };
        let Some(p) = progress.get_mut(&f) else { return };
        if p.inflight || p.next > last {
            return;
        }
        p.inflight = true;
        p.inflight_ticks = 0;
        let prev_index = p.next - 1;
        if prev_index < snapshot.index {
            // The entries it needs are compacted: send the snapshot.
            self.outbox.push(Send {
                to: f,
                msg: Message::Snapshot { term, meta: snapshot, data: Bytes::new(), seq },
                fill_from: None,
                attach_snapshot: true,
            });
            return;
        }
        let fill = p.next;
        let prev_term = self.log.term_at(prev_index).expect("prev is in the log");
        self.outbox.push(Send {
            to: f,
            msg: Message::Append { term, prev_index, prev_term, entries: Vec::new(), commit, seq },
            fill_from: Some(fill),
            attach_snapshot: false,
        });
    }

    fn replicate_or_heartbeat(&mut self, f: NodeId) {
        let last = self.log.last_index();
        let (term, commit, seq) = (self.term, self.commit, self.seq);
        let State::Leader { progress } = &self.state else { return };
        let Some(p) = progress.get(&f) else { return };
        if !p.inflight && p.next <= last {
            self.replicate(f);
            return;
        }
        // A heartbeat names an index the follower is known to have, or the
        // snapshot point if that is compacted (then a mismatch only means a
        // rejection the leader ignores).
        let prev_index = p.matched.max(self.log.snapshot_index());
        let prev_term = self.log.term_at(prev_index).expect("matched is in the log");
        self.send(
            f,
            Message::Append { term, prev_index, prev_term, entries: Vec::new(), commit: commit.min(prev_index), seq },
        );
    }

    /// Moves `index` back to an entry boundary of this log that is not
    /// below what `f` is known to have. At or before the snapshot it stays:
    /// the follower then gets the snapshot.
    fn boundary(&self, index: u64, matched: u64) -> u64 {
        let last = self.log.last_index();
        if index > last {
            return last + 1;
        }
        if index == matched + 1 || index <= self.log.snapshot_index() {
            return index;
        }
        self.log.run_start(index).max(matched + 1)
    }

    fn on_append_reply(&mut self, from: NodeId, result: AppendResult, seq: u64) {
        let State::Leader { progress } = &self.state else { return };
        let Some(p) = progress.get(&from).cloned() else { return };
        let mut p = p;
        p.active = true;
        p.acked = p.acked.max(seq);
        match result {
            AppendResult::Accepted { last } => {
                // A heartbeat's answer (last < next) says nothing about the
                // append in flight; only an answer covering it clears it.
                if last >= p.next {
                    p.inflight = false;
                }
                if last > p.matched {
                    p.matched = last;
                }
                p.next = p.next.max(p.matched + 1);
            }
            AppendResult::Rejected { prev_index, last_index, conflict } => {
                // Only the answer to the append in flight moves `next`.
                if prev_index + 1 != p.next || prev_index < p.matched {
                    self.put_progress(from, p);
                    self.check_reads();
                    return;
                }
                p.inflight = false;
                let candidate = match conflict {
                    Some((t, first)) => match self.log.last_of_term(t) {
                        Some(end) => end + 1,
                        None => first,
                    },
                    None => last_index + 1,
                };
                let candidate = candidate.min(prev_index).max(p.matched + 1);
                p.next = self.boundary(candidate, p.matched);
            }
        }
        self.put_progress(from, p);
        self.check_reads();
        self.maybe_commit();
        self.replicate(from);
        self.maybe_timeout_now(from);
    }

    fn put_progress(&mut self, f: NodeId, p: Progress) {
        if let State::Leader { progress } = &mut self.state {
            progress.insert(f, p);
        }
    }

    fn maybe_commit(&mut self) {
        let State::Leader { progress } = &self.state else { return };
        let me = self.cfg.id;
        let persisted = self.persisted;
        let n = self
            .membership()
            .committed(|v| if v == me { persisted } else { progress.get(&v).map_or(0, |p| p.matched) });
        // Raft §5.4.2: only entries of the current term commit by counting.
        if n > self.commit && self.log.term_at(n) == Some(self.term) {
            self.commit = n;
            self.hs_dirty = true;
            // Followers learn the new commit index with the next append or
            // heartbeat.
            for ctx in std::mem::take(&mut self.reads_waiting) {
                self.register_read(ctx);
            }
        }
        self.after_commit();
    }

    /// Finishes a membership change once its entry is committed: leaves a
    /// joint configuration, and steps down a leader that is no longer a voter.
    fn after_commit(&mut self) {
        if !matches!(self.state, State::Leader { .. }) || self.membership_index() > self.commit {
            return;
        }
        let m = self.membership().clone();
        if m.is_joint() {
            self.append_config(Membership {
                voters: m.voters,
                outgoing: BTreeSet::new(),
                learners: m.learners,
                tag: m.tag,
            });
            for f in self.followers() {
                self.replicate(f);
            }
        } else if !m.is_voter(self.cfg.id) {
            // Tell everyone the final commit index before leaving.
            for f in self.followers() {
                self.replicate_or_heartbeat(f);
            }
            let term = self.term;
            self.become_follower(term, None);
        }
    }

    // ----- messages -----------------------------------------------------

    /// Handles a message from `from`.
    pub fn step(&mut self, from: NodeId, msg: Message) {
        let mterm = msg.term();
        // Votes are not filtered by membership: a lagging voter may not know
        // a newly promoted one yet and must still be able to elect it. A
        // removed node that keeps campaigning is held off by pre-vote (its
        // log is behind) and leader stickiness.
        if mterm > self.term {
            if let Message::Vote { transfer, .. } = msg {
                // Leader stickiness: while we hear from a leader, nobody can
                // take over by asking for votes, unless the leader asked.
                let in_lease = self.leader.is_some() && self.election_elapsed < self.cfg.election_ticks;
                // With leases, a node that just started may have forgotten
                // such a leader: it waits an election timeout.
                if (in_lease && !transfer) || self.quiet > 0 {
                    return;
                }
            }
            match &msg {
                Message::Vote { pre: true, .. } => {}
                Message::VoteReply { pre: true, granted: true, .. } => {}
                Message::Append { .. } | Message::Snapshot { .. } => self.become_follower(mterm, Some(from)),
                _ => self.become_follower(mterm, None),
            }
        } else if mterm < self.term {
            match msg {
                Message::Append { prev_index, seq, .. } => {
                    let last_index = self.log.last_index();
                    let term = self.term;
                    self.send(
                        from,
                        Message::AppendReply {
                            term,
                            result: AppendResult::Rejected { prev_index, last_index, conflict: None },
                            seq,
                        },
                    );
                }
                Message::Snapshot { meta, seq, .. } => {
                    let last_index = self.log.last_index();
                    let term = self.term;
                    let result = AppendResult::Rejected { prev_index: meta.index, last_index, conflict: None };
                    self.send(from, Message::AppendReply { term, result, seq });
                }
                Message::Vote { pre, .. } => {
                    let term = self.term;
                    self.send(from, Message::VoteReply { term, granted: false, pre });
                }
                _ => {}
            }
            return;
        }

        match msg {
            Message::Vote { term, last_index, last_term, pre, .. } => {
                self.on_vote(from, term, last_index, last_term, pre)
            }
            Message::VoteReply { term, granted, pre } => self.on_vote_reply(from, term, granted, pre),
            Message::Append { term, prev_index, prev_term, entries, commit, seq } => {
                self.follow(term, from);
                self.on_append(from, prev_index, prev_term, entries, commit, seq);
            }
            Message::Snapshot { term, meta, data, seq } => {
                self.follow(term, from);
                self.on_snapshot(from, meta, data, seq);
            }
            Message::AppendReply { result, seq, .. } => self.on_append_reply(from, result, seq),
            Message::TimeoutNow { .. } => {
                // The leader hands over: campaign now, skipping pre-vote.
                if !matches!(self.state, State::Leader { .. }) && self.can_campaign() {
                    self.campaign_with(true);
                }
            }
        }
    }

    fn follow(&mut self, term: u64, leader: NodeId) {
        if !matches!(self.state, State::Follower) {
            self.become_follower(term, Some(leader));
        }
        self.leader = Some(leader);
        self.election_elapsed = 0;
    }

    fn up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        let (my_index, my_term) = (self.log.last_index(), self.log.last_term());
        last_term > my_term || (last_term == my_term && last_index >= my_index)
    }

    fn on_vote(&mut self, from: NodeId, term: u64, last_index: u64, last_term: u64, pre: bool) {
        let up = self.up_to_date(last_index, last_term);
        let granted = if pre {
            up && term > self.term
        } else {
            up && (self.vote.is_none() || self.vote == Some(from)) && self.leader.is_none_or(|l| l == from)
        };
        if granted && !pre {
            self.vote = Some(from);
            self.hs_dirty = true;
            self.election_elapsed = 0;
        }
        let reply_term = if pre && granted { term } else { self.term };
        self.send(from, Message::VoteReply { term: reply_term, granted, pre });
    }

    fn on_vote_reply(&mut self, from: NodeId, term: u64, granted: bool, pre: bool) {
        let expected = if pre { self.term + 1 } else { self.term };
        let membership = self.membership().clone();
        let (g, r) = match &mut self.state {
            State::PreCandidate { granted: g, rejected: r } if pre => (g, r),
            State::Candidate { granted: g, rejected: r } if !pre => (g, r),
            _ => return,
        };
        if !membership.is_voter(from) {
            return;
        }
        if granted && term == expected {
            g.insert(from);
        } else if !granted {
            r.insert(from);
        }
        let won = membership.has_quorum(g);
        // Lost once even every voter that has not said no would not suffice.
        let maybe: BTreeSet<NodeId> = membership.all_voters().difference(r).copied().collect();
        let lost = !membership.has_quorum(&maybe);
        if won {
            if pre {
                self.campaign();
            } else {
                self.become_leader();
            }
        } else if lost {
            let t = self.term;
            self.become_follower(t, None);
        }
    }

    fn on_append(
        &mut self,
        from: NodeId,
        mut prev_index: u64,
        mut prev_term: u64,
        mut entries: Vec<Entry>,
        commit: u64,
        seq: u64,
    ) {
        let term = self.term;
        let last_index = self.log.last_index();
        if prev_index > last_index {
            self.send(
                from,
                Message::AppendReply {
                    term,
                    result: AppendResult::Rejected { prev_index, last_index, conflict: None },
                    seq,
                },
            );
            return;
        }
        let snap = self.log.snapshot_index();
        if prev_index < snap {
            // We compacted past `prev_index`: everything up to the snapshot
            // is committed, so it matches; skip what the snapshot covers.
            entries.retain(|e| e.first > snap);
            prev_index = snap;
            prev_term = self.log.snapshot_term();
            if entries.first().is_some_and(|e| e.first != snap + 1) {
                entries.clear();
            }
        }
        let have = self.log.term_at(prev_index).expect("prev is within the log");
        if have != prev_term {
            let first = self.log.run_start(prev_index);
            self.send(
                from,
                Message::AppendReply {
                    term,
                    result: AppendResult::Rejected { prev_index, last_index, conflict: Some((have, first)) },
                    seq,
                },
            );
            return;
        }
        let mut last_new = prev_index;
        for e in entries {
            if e.first != last_new + 1 {
                break; // not contiguous: a malformed message, ignore the rest
            }
            match self.log.term_at(e.first) {
                Some(t) if t == e.term => {
                    // Already have it: same index and term means the same batch.
                    last_new = e.last;
                }
                Some(_) => {
                    assert!(e.first > self.commit, "a leader asked to overwrite committed index {}", e.first);
                    self.truncate_from(e.first);
                    last_new = e.last;
                    self.append_received(e);
                }
                None => {
                    last_new = e.last;
                    self.append_received(e);
                }
            }
        }
        let new_commit = commit.min(last_new);
        if new_commit > self.commit {
            self.commit = new_commit;
            self.hs_dirty = true;
        }
        self.send(from, Message::AppendReply { term, result: AppendResult::Accepted { last: last_new }, seq });
    }

    fn on_snapshot(&mut self, from: NodeId, meta: SnapshotMeta, data: Bytes, seq: u64) {
        let term = self.term;
        let index = meta.index;
        if index <= self.commit {
            // Nothing new: we already have all of it.
        } else if self.log.term_at(index) == Some(meta.term) {
            // Our log holds the snapshot's last entry, so it matches the
            // leader's up to there. Keep it (entries after it may be acked
            // copies of committed entries) and just commit.
            self.commit = index;
            self.hs_dirty = true;
        } else {
            self.log = TermRuns::with_snapshot(index, meta.term);
            self.base_membership = meta.membership.clone();
            self.configs.clear();
            self.pending_entries.clear();
            self.pending_truncate = None;
            self.commit = index;
            self.applied = index;
            self.persisted = index;
            self.ready_last = index;
            self.hs_dirty = true;
            self.pending_snapshot = Some(Snapshot { meta, data });
        }
        // Either way the log now matches the leader's up to `index`.
        self.send(from, Message::AppendReply { term, result: AppendResult::Accepted { last: index }, seq });
    }

    fn truncate_from(&mut self, index: u64) {
        self.log.truncate_from(index);
        self.configs.retain(|c| c.0 < index);
        self.pending_entries.retain(|e| e.first < index);
        if index <= self.persisted.max(self.ready_last) {
            self.pending_truncate = Some(self.pending_truncate.map_or(index, |t| t.min(index)));
        }
        self.persisted = self.persisted.min(index - 1);
        self.ready_last = self.ready_last.min(index - 1);
    }

    fn append_received(&mut self, e: Entry) {
        self.log.append(e.first, e.last, e.term);
        if e.kind == EntryKind::Config {
            let m = Membership::decode(&e.data).expect("config entry holds a membership");
            self.configs.push((e.last, m));
        }
        self.pending_entries.push(e);
    }

    // ----- ready / advance ---------------------------------------------

    /// `true` if [`RaftGroup::ready`] has something.
    pub fn has_ready(&self) -> bool {
        self.hs_dirty
            || self.pending_snapshot.is_some()
            || self.pending_truncate.is_some()
            || !self.pending_entries.is_empty()
            || !self.outbox.is_empty()
            || self.need_round
            || !self.reads_ready.is_empty()
            || !self.reads_aborted.is_empty()
            || self.commit > self.applied
    }

    /// Takes everything the driver has to do. See [`Ready`] for the order.
    pub fn ready(&mut self) -> Ready {
        // Every read that arrived since the last ready shares one round.
        if self.need_round && matches!(self.state, State::Leader { .. }) {
            self.start_round();
        }
        let hard_state = std::mem::take(&mut self.hs_dirty).then_some(HardState {
            term: self.term,
            vote: self.vote,
            commit: self.commit,
        });
        let entries = std::mem::take(&mut self.pending_entries);
        if let Some(e) = entries.last() {
            self.ready_last = e.last;
        } else if self.pending_truncate.is_some() {
            self.ready_last = self.log.last_index().min(self.ready_last);
        }
        let committed = (self.commit > self.applied).then(|| (self.applied + 1, self.commit));
        self.applied = self.commit;
        Ready {
            hard_state,
            snapshot: self.pending_snapshot.take(),
            truncate_from: self.pending_truncate.take(),
            entries,
            committed,
            reads: std::mem::take(&mut self.reads_ready),
            reads_aborted: std::mem::take(&mut self.reads_aborted),
            messages: std::mem::take(&mut self.outbox),
        }
    }

    /// Tells the group that the last [`Ready`] is durable.
    pub fn advance(&mut self) {
        if self.ready_last > self.persisted {
            self.persisted = self.ready_last;
        }
        if matches!(self.state, State::Leader { .. }) {
            self.maybe_commit();
        }
    }
}

#[cfg(test)]
mod tests;
