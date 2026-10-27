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
//! # What is in
//!
//! Leader election with pre-vote, check-quorum and leader stickiness (a node
//! that heard from a leader recently ignores vote requests), log replication
//! with one in-flight append per follower and conflict-term backtracking, a
//! no-op entry when a leader takes over so earlier entries commit, and the
//! commit rule that only counts entries of the leader's own term.
//!
//! # Not yet
//!
//! Membership changes (joint consensus), snapshots and log compaction
//! (`InstallSnapshot`), leader transfer and read leases.

#![warn(missing_docs)]

mod multi;
mod terms;
pub mod testing;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

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
    },
    /// Answer to [`Message::Append`].
    AppendReply {
        /// Follower's term.
        term: u64,
        /// Outcome.
        result: AppendResult,
    },
}

impl Message {
    /// Term carried by the message.
    pub fn term(&self) -> u64 {
        match self {
            Message::Vote { term, .. }
            | Message::VoteReply { term, .. }
            | Message::Append { term, .. }
            | Message::AppendReply { term, .. } => *term,
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
    /// but at least one.
    pub fill_from: Option<u64>,
}

/// Everything the driver has to do, in this order: persist `hard_state`,
/// truncate the log at `truncate_from`, append `entries`, apply the
/// `committed` range, then send `messages`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ready {
    /// New hard state, if it changed.
    pub hard_state: Option<HardState>,
    /// Remove every index from here on before appending.
    pub truncate_from: Option<u64>,
    /// Entries to append, in order.
    pub entries: Vec<Entry>,
    /// Newly committed indices `(first, last)`, to apply in order.
    pub committed: Option<(u64, u64)>,
    /// Messages to send once the above is durable.
    pub messages: Vec<Send>,
}

impl Ready {
    /// `true` if there is nothing to do.
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.truncate_from.is_none()
            && self.entries.is_empty()
            && self.committed.is_none()
            && self.messages.is_empty()
    }
}

/// Timing and behaviour.
#[derive(Debug, Clone)]
pub struct Config {
    /// This node.
    pub id: NodeId,
    /// All voters of the group, this node included.
    pub voters: Vec<NodeId>,
    /// Minimum ticks without a leader before an election. The actual
    /// timeout is random in `election_ticks..2 * election_ticks`.
    pub election_ticks: u32,
    /// Ticks between heartbeats.
    pub heartbeat_ticks: u32,
    /// Ask with a pre-vote before raising the term.
    pub pre_vote: bool,
    /// A leader steps down when it has not heard from a quorum for an
    /// election timeout, and followers ignore vote requests while they
    /// hear from their leader.
    pub check_quorum: bool,
    /// Seed for the randomised election timeout.
    pub seed: u64,
}

impl Config {
    /// Defaults for node `id` among `voters`: 10-tick elections,
    /// heartbeats every 2 ticks, pre-vote and check-quorum on.
    pub fn new(id: NodeId, voters: Vec<NodeId>) -> Self {
        Config { id, voters, election_ticks: 10, heartbeat_ticks: 2, pre_vote: true, check_quorum: true, seed: id }
    }
}

/// What a node knows after a restart.
#[derive(Debug, Clone, Default)]
pub struct Restored {
    /// Last persisted hard state.
    pub hard_state: HardState,
    /// Terms of the persisted log.
    pub log: TermRuns,
    /// Highest index the driver has applied.
    pub applied: u64,
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

#[derive(Debug, Clone)]
struct Progress {
    matched: u64,
    next: u64,
    inflight: bool,
    inflight_ticks: u32,
    active: bool,
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
    voters: BTreeSet<NodeId>,
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
    pending_truncate: Option<u64>,
    pending_entries: Vec<Entry>,
    outbox: Vec<Send>,
}

impl RaftGroup {
    /// A node of a group, from what it persisted (a fresh node uses
    /// `Restored::default()`).
    pub fn new(cfg: Config, restored: Restored) -> Self {
        assert!(cfg.election_ticks > cfg.heartbeat_ticks, "elections must take longer than heartbeats");
        let voters: BTreeSet<NodeId> = cfg.voters.iter().copied().collect();
        let last = restored.log.last_index();
        let mut g = RaftGroup {
            rng: cfg.seed ^ 0x9E37_79B9_7F4A_7C15,
            voters,
            term: restored.hard_state.term,
            vote: restored.hard_state.vote,
            commit: restored.hard_state.commit.min(last),
            applied: restored.applied.min(restored.hard_state.commit.min(last)),
            leader: None,
            state: State::Follower,
            log: restored.log,
            persisted: last,
            ready_last: last,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            timeout: 0,
            hs_dirty: false,
            pending_truncate: None,
            pending_entries: Vec::new(),
            outbox: Vec::new(),
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

    fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
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
        self.outbox.push(Send { to, msg, fill_from: None });
    }

    fn others(&self) -> Vec<NodeId> {
        self.voters.iter().copied().filter(|v| *v != self.cfg.id).collect()
    }

    // ----- time ---------------------------------------------------------

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        if matches!(self.state, State::Leader { .. }) {
            self.tick_leader();
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.timeout && self.voters.contains(&self.cfg.id) {
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
        if self.cfg.check_quorum && self.election_elapsed >= election {
            self.election_elapsed = 0;
            let quorum = self.quorum();
            if let State::Leader { progress } = &mut self.state {
                let active = 1 + progress.values().filter(|p| p.active).count();
                progress.values_mut().for_each(|p| p.active = false);
                if active < quorum {
                    let term = self.term;
                    self.become_follower(term, None);
                    return;
                }
            }
        }
        if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
            self.heartbeat_elapsed = 0;
            for f in self.others() {
                self.replicate_or_heartbeat(f);
            }
        }
    }

    // ----- role changes -------------------------------------------------

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
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
        self.state = State::PreCandidate { granted: BTreeSet::from([self.cfg.id]), rejected: BTreeSet::new() };
        self.leader = None;
        self.reset_timeout();
        if self.quorum() == 1 {
            self.campaign();
            return;
        }
        let (last_index, last_term) = (self.log.last_index(), self.log.last_term());
        let term = self.term + 1;
        for v in self.others() {
            self.send(v, Message::Vote { term, last_index, last_term, pre: true });
        }
    }

    fn campaign(&mut self) {
        self.term += 1;
        self.vote = Some(self.cfg.id);
        self.hs_dirty = true;
        self.leader = None;
        self.state = State::Candidate { granted: BTreeSet::from([self.cfg.id]), rejected: BTreeSet::new() };
        self.election_elapsed = 0;
        self.reset_timeout();
        if self.quorum() == 1 {
            self.become_leader();
            return;
        }
        let (last_index, last_term, term) = (self.log.last_index(), self.log.last_term(), self.term);
        for v in self.others() {
            self.send(v, Message::Vote { term, last_index, last_term, pre: false });
        }
    }

    fn become_leader(&mut self) {
        let next = self.log.last_index() + 1;
        let progress = self
            .others()
            .into_iter()
            .map(|f| (f, Progress { matched: 0, next, inflight: false, inflight_ticks: 0, active: true }))
            .collect();
        self.state = State::Leader { progress };
        self.leader = Some(self.cfg.id);
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        // Entries of earlier terms only commit through an entry of this term.
        self.append_own(1, EntryKind::Noop, Bytes::new());
        for f in self.others() {
            self.replicate_or_heartbeat(f);
        }
        self.maybe_commit();
    }

    // ----- proposals ----------------------------------------------------

    /// Appends a batch of `records` records as the leader. Returns the
    /// indices it occupies; the driver stamps the batch with
    /// `base_offset = first - 1` and the current term.
    pub fn propose(&mut self, records: u64, data: Bytes) -> Result<(u64, u64), NotLeader> {
        if !matches!(self.state, State::Leader { .. }) {
            return Err(NotLeader { leader: self.leader });
        }
        let range = self.append_own(records.max(1), EntryKind::Normal, data);
        for f in self.others() {
            self.replicate(f);
        }
        Ok(range)
    }

    fn append_own(&mut self, records: u64, kind: EntryKind, data: Bytes) -> (u64, u64) {
        let first = self.log.last_index() + 1;
        let last = first + records - 1;
        self.log.append(first, last, self.term);
        self.pending_entries.push(Entry { first, last, term: self.term, kind, data });
        (first, last)
    }

    // ----- replication (leader side) -------------------------------------

    fn replicate(&mut self, f: NodeId) {
        let last = self.log.last_index();
        let (term, commit) = (self.term, self.commit);
        let State::Leader { progress } = &mut self.state else { return };
        let Some(p) = progress.get_mut(&f) else { return };
        if p.inflight || p.next > last {
            return;
        }
        let prev_index = p.next - 1;
        let fill = p.next;
        p.inflight = true;
        p.inflight_ticks = 0;
        let prev_term = self.log.term_at(prev_index).expect("prev is in the log");
        self.outbox.push(Send {
            to: f,
            msg: Message::Append { term, prev_index, prev_term, entries: Vec::new(), commit },
            fill_from: Some(fill),
        });
    }

    fn replicate_or_heartbeat(&mut self, f: NodeId) {
        let last = self.log.last_index();
        let (term, commit) = (self.term, self.commit);
        let State::Leader { progress } = &self.state else { return };
        let Some(p) = progress.get(&f) else { return };
        if !p.inflight && p.next <= last {
            self.replicate(f);
            return;
        }
        let prev_index = p.matched;
        let prev_term = self.log.term_at(prev_index).expect("matched is in the log");
        self.send(
            f,
            Message::Append { term, prev_index, prev_term, entries: Vec::new(), commit: commit.min(prev_index) },
        );
    }

    /// Moves `index` back to an entry boundary of this log that is not
    /// below what `f` is known to have.
    fn boundary(&self, index: u64, matched: u64) -> u64 {
        let last = self.log.last_index();
        if index > last {
            return last + 1;
        }
        if index == matched + 1 {
            return index;
        }
        self.log.run_start(index).max(matched + 1)
    }

    fn on_append_reply(&mut self, from: NodeId, result: AppendResult) {
        let State::Leader { progress } = &self.state else { return };
        let Some(p) = progress.get(&from).cloned() else { return };
        let mut p = p;
        p.active = true;
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
        self.maybe_commit();
        self.replicate(from);
    }

    fn put_progress(&mut self, f: NodeId, p: Progress) {
        if let State::Leader { progress } = &mut self.state {
            progress.insert(f, p);
        }
    }

    fn maybe_commit(&mut self) {
        let State::Leader { progress } = &self.state else { return };
        let mut matched: Vec<u64> =
            self.voters.iter().map(|v| if *v == self.cfg.id { self.persisted } else { progress[v].matched }).collect();
        matched.sort_unstable();
        let n = matched[matched.len() - self.quorum()];
        // Raft §5.4.2: only entries of the current term commit by counting.
        if n > self.commit && self.log.term_at(n) == Some(self.term) {
            self.commit = n;
            self.hs_dirty = true;
        }
    }

    // ----- messages -----------------------------------------------------

    /// Handles a message from `from`.
    pub fn step(&mut self, from: NodeId, msg: Message) {
        let mterm = msg.term();
        if mterm > self.term {
            if let Message::Vote { .. } = msg {
                // Leader stickiness: while we hear from a leader, nobody can
                // take over by asking for votes.
                let in_lease =
                    self.cfg.check_quorum && self.leader.is_some() && self.election_elapsed < self.cfg.election_ticks;
                if in_lease {
                    return;
                }
            }
            match &msg {
                Message::Vote { pre: true, .. } => {}
                Message::VoteReply { pre: true, granted: true, .. } => {}
                Message::Append { .. } => self.become_follower(mterm, Some(from)),
                _ => self.become_follower(mterm, None),
            }
        } else if mterm < self.term {
            match msg {
                Message::Append { prev_index, .. } => {
                    let last_index = self.log.last_index();
                    let term = self.term;
                    self.send(
                        from,
                        Message::AppendReply {
                            term,
                            result: AppendResult::Rejected { prev_index, last_index, conflict: None },
                        },
                    );
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
            Message::Vote { term, last_index, last_term, pre } => self.on_vote(from, term, last_index, last_term, pre),
            Message::VoteReply { term, granted, pre } => self.on_vote_reply(from, term, granted, pre),
            Message::Append { term, prev_index, prev_term, entries, commit } => {
                if !matches!(self.state, State::Follower) {
                    self.become_follower(term, Some(from));
                }
                self.leader = Some(from);
                self.election_elapsed = 0;
                self.on_append(from, prev_index, prev_term, entries, commit);
            }
            Message::AppendReply { result, .. } => self.on_append_reply(from, result),
        }
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
        let quorum = self.quorum();
        let expected = if pre { self.term + 1 } else { self.term };
        let (g, r) = match &mut self.state {
            State::PreCandidate { granted: g, rejected: r } if pre => (g, r),
            State::Candidate { granted: g, rejected: r } if !pre => (g, r),
            _ => return,
        };
        if granted && term == expected {
            g.insert(from);
        } else if !granted {
            r.insert(from);
        }
        let (won, lost) = (g.len() >= quorum, r.len() >= quorum);
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

    fn on_append(&mut self, from: NodeId, prev_index: u64, prev_term: u64, entries: Vec<Entry>, commit: u64) {
        let term = self.term;
        let last_index = self.log.last_index();
        if prev_index > last_index {
            self.send(
                from,
                Message::AppendReply {
                    term,
                    result: AppendResult::Rejected { prev_index, last_index, conflict: None },
                },
            );
            return;
        }
        let have = self.log.term_at(prev_index).expect("prev is within the log");
        if have != prev_term {
            let first = self.log.run_start(prev_index);
            self.send(
                from,
                Message::AppendReply {
                    term,
                    result: AppendResult::Rejected { prev_index, last_index, conflict: Some((have, first)) },
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
        self.send(from, Message::AppendReply { term, result: AppendResult::Accepted { last: last_new } });
    }

    fn truncate_from(&mut self, index: u64) {
        self.log.truncate_from(index);
        self.pending_entries.retain(|e| e.first < index);
        if index <= self.persisted.max(self.ready_last) {
            self.pending_truncate = Some(self.pending_truncate.map_or(index, |t| t.min(index)));
        }
        self.persisted = self.persisted.min(index - 1);
        self.ready_last = self.ready_last.min(index - 1);
    }

    fn append_received(&mut self, e: Entry) {
        self.log.append(e.first, e.last, e.term);
        self.pending_entries.push(e);
    }

    // ----- ready / advance ---------------------------------------------

    /// `true` if [`RaftGroup::ready`] has something.
    pub fn has_ready(&self) -> bool {
        self.hs_dirty
            || self.pending_truncate.is_some()
            || !self.pending_entries.is_empty()
            || !self.outbox.is_empty()
            || self.commit > self.applied
    }

    /// Takes everything the driver has to do. See [`Ready`] for the order.
    pub fn ready(&mut self) -> Ready {
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
            truncate_from: self.pending_truncate.take(),
            entries,
            committed,
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
