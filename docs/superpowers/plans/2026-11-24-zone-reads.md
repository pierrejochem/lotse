# Reads in the Client's Zone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A reader that fell back to the leader returns to its zone when the leader names a caught-up replica there. Committed offsets and admin reads are served in the client's zone with read-your-writes.

**Architecture:**
- **Fetch.** Gains a `zone` tag. A leader answers it with `preferred_replica` when a voter in that zone is current.
- **Frame-header tags.** A response carries `applied` (group, index) pairs, and a read request carries `read_at_least` pairs. These live in the existing header tag fields, so no per-message change is needed.
- **Where reads go.** A node serves a tagged read from its applied state once it has applied that far and is not lagging. Clients keep the highest index per group and send zone reads to a replica in their zone. The coordinator's voters come from a new `FindCoordinator` tag.

**Tech Stack:** Rust (`lotse-proto`, `lotse-raft`, `lotse-server`, `lotse-client`, `lotse-node`, `lotse-mcp`), Java 17 and Kotlin, C# (.NET 10), TypeScript (Node.js 20+).

**Spec:** `docs/superpowers/specs/2026-11-24-zone-reads-design.md` (approved).

## Global Constraints

- **Determinism.** Nothing above `lotse-rt` reads the real clock or uses `HashMap`/`HashSet`; time comes from `Clock`, and maps are `BTreeMap`/`BTreeSet` (AGENTS rule 1). `unsafe` is forbidden.
- **The current rule.** A replica is current when it hears its leader within an election timeout and caught up within `follower_max_lag` (10 s default, `cluster.follower_max_lag_ms`). The leader judges its voters by the same rule from its side (ADR 0116).
- **Tags.**
  - `Fetch` request tag 1 `zone` (string).
  - `FetchPartitionResponse` tag 1 `preferred_replica` (node id, `u32`).
  - `FindCoordinatorResponse` tag 0 `voters` (`u32` count, then `u32` node ids).
  - Request header tag 0 `read_at_least` and response header tag 0 `applied`. Both are encoded as `u32` count, then {group `u64`, index `u64`} per entry, little-endian like every other field.
- **Groups.** Metadata group id 1, coordinator group id 2. A client's index per group only ever rises.
- **Requests.**
  - Zone reads, the only requests that carry `read_at_least`: `OffsetFetch`, `DescribeConfigs`, `DescribeNodes`, `DescribeUsers`, `DescribeTokens`, `DescribeCertificates`, `DescribeCertificateRequests`, `DescribeUpgrade`, `DescribeTenants`, `DescribeRoleBindings`.
  - `Metadata` carries `applied` but is never refused (ruling below).
- **Compatibility.** A request without the tags behaves exactly as before. Older nodes ignore the tags. Nothing is stored, so no cluster-version gate applies.
- **Workflow.**
  - Errors use `thiserror`. Comments say why. Code, docs and commits are in English, except `docs/design.md`, which stays in German.
  - Commit as `git -c user.name="Pierre Jochem" -c user.email="pierrejochem@msn.com" commit`, with an imperative subject, a body that says why, and no AI trailers.
  - Rust commands use `CARGO_TARGET_DIR=/home/claude/target-snap CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0`.
- **Ruling: `Metadata` gains no refusal.** Its answer has no top-level error field, and all four clients would need one. It carries `applied` like every other answer from applied state. A client already retries until a resource it created shows up. If this ruling is wrong, a later change adds the refusal with an error field.

## Review Focus

1. **An idle client.** A client that has seen no `applied` sends index 0 for both groups. A zone node answers it from applied state while current. Test in Task 4: `a_read_at_zero_is_served_by_a_current_follower`.
2. **A coordinator move.** The coordinator's leader moves while a member reads offsets in its zone. A zone voter that has not applied the new leader's commits refuses, and the read goes to the new leader. Test in Task 7, simulation oracle (b).
3. **A refused zone read must not stick.** A zone node that refuses once must not stop the next read from trying the zone again. Each SDK tests two reads in a row: the first refused and served by the leader, the second served by the zone (Tasks 6, 9, 10, 11).
4. **A mixed cluster.** A zone node that is an older node ignores `read_at_least`. On the leader-only reads it answers `NOT_LEADER`, and the client must go to the leader rather than retry the zone in a loop. Test in Task 6: `an_older_zone_node_sends_the_read_to_the_leader`.
5. **The leader names its own zone.** When the leader is itself in the client's zone, it must name no `preferred_replica`. The client already reads from it, and naming another voter would move the reader off a local leader. Test in Task 3: `a_leader_in_the_zone_names_no_other_replica`.

---

### Task 1: Protocol: tags, frame-header tags, vectors

**Files:**
- Modify: `crates/lotse-proto/src/lib.rs` (frame encode and decode), `crates/lotse-proto/src/messages.rs` (`FetchRequest`, `FetchPartitionResponse`, `FindCoordinatorResponse`), `crates/lotse-proto/src/tests.rs`, `crates/lotse-proto/examples/vectors.rs`, `sdk/conformance/*` (regenerated), `sdk/PROTOCOL.md`

**Interfaces:**
- Produces:
  - `pub struct GroupIndex { pub group: u64, pub index: u64 }` (`Copy`, `Ord`).
  - `pub struct FrameTags { pub read_at_least: Vec<GroupIndex>, pub applied: Vec<GroupIndex> }` (`Default`).
  - `pub fn encode_request_tagged(correlation_id: u32, tags: &FrameTags, request: &Request) -> Bytes`
  - `pub fn decode_request_tagged(frame: Bytes) -> Result<(RequestHeader, FrameTags, Request), DecodeError>`
  - `pub fn encode_response_tagged(correlation_id: u32, tags: &FrameTags, response: &Response) -> Bytes`
  - `pub fn decode_response_tagged(frame: Bytes, api_key: ApiKey) -> Result<(u32, FrameTags, Response), DecodeError>`

  The existing four functions stay as wrappers with empty tags.
- Field changes:
  - `FetchRequest.zone: Option<String>`
  - `FetchPartitionResponse.preferred_replica: Option<u32>`
  - `FindCoordinatorResponse.voters: Vec<u32>` (empty is encoded as no tag)

- [ ] **Step 1: Write the failing round-trip tests** in `crates/lotse-proto/src/tests.rs`:
  - `frame_tags_round_trip`: a request with `read_at_least = [{1, 7}, {2, u64::MAX}]` and a response with `applied = [{1, 9}]` decode to the same tags. A frame without them decodes to `FrameTags::default()`.
  - `fetch_zone_and_preferred_replica_round_trip`: `zone = Some("eu-1")` and `preferred_replica = Some(3)` survive. `None` is encoded as no tag, so the bytes equal today's encoding of the same message.
  - `find_coordinator_voters_round_trip`: `voters = [1, 3, 5]` survives, and empty encodes as before.
- [ ] **Step 2: Run them and see them fail.** `cargo test -p lotse-proto frame_tags fetch_zone find_coordinator_voters` fails to compile.
- [ ] **Step 3: Implement the types, the tagged functions and the three message tags.** An unknown header tag is skipped, as unknown message tags are.
- [ ] **Step 4: Add golden vectors** in `examples/vectors.rs`: one request with `read_at_least`, one response with `applied`, a `Fetch` with `zone`, a `FetchResponse` with `preferred_replica`, and a `FindCoordinatorResponse` with `voters`. Run `make sdk-vectors`, and document the header tags and the three message tags in `sdk/PROTOCOL.md`.
- [ ] **Step 5: Verify.** `cargo test -p lotse-proto`, `cargo clippy -p lotse-proto --all-targets --features samples -- -D warnings` and `make sdk-vectors` must all pass, and `git diff --stat sdk/conformance` must show only new samples.
- [ ] **Step 6: Commit** with "Carry zones, preferred replicas, coordinator voters and read indexes in the protocol".

### Task 2: Raft: the leader's view of a voter

**Files:**
- Modify: `crates/lotse-raft/src/lib.rs`, `crates/lotse-raft/src/tests.rs`

**Interfaces:**
- Produces: `pub fn voter_progress(&self, id: NodeId) -> Option<VoterProgress>` on the Raft core. It is `None` unless this node leads and `id` is a member other than itself.
- `pub struct VoterProgress { pub matched: u64, pub heard_recently: bool }`. `heard_recently` means its latest answer came within an election timeout, measured in ticks.

- [ ] **Step 1: Write the failing test.** `leader_sees_a_voter_s_match_and_whether_it_hears_it`:
  - On a three-node `MemNode` cluster, after a commit, `voter_progress(2)` gives `matched == commit_index()` and `heard_recently`.
  - After node 2 is cut off for more than an election timeout of ticks, `heard_recently` is false.
  - On a follower, `voter_progress(1)` is `None`.
- [ ] **Step 2: Run it and see it fail.** `cargo test -p lotse-raft leader_sees_a_voter` fails to compile.
- [ ] **Step 3: Implement it** from `Progress.matched` and `Progress.heard_at`.
- [ ] **Step 4: Verify.** `cargo test -p lotse-raft` passes.
- [ ] **Step 5: Commit** with "Let a leader say how far a voter has come and whether it hears it".

### Task 3: Server: the leader points back

**Files:**
- Modify: `crates/lotse-server/src/node.rs` (per-group, per-voter "caught up at" times, next to the existing `caught_up` for the node itself), the `Fetch` serving in `crates/lotse-server/src/client.rs` (or the file that holds it), `crates/lotse-server/src/tests.rs`

**Interfaces:**
- Consumes: `voter_progress` (Task 2); `FetchRequest.zone` and `FetchPartitionResponse.preferred_replica` (Task 1).
- Produces: `fn preferred_replica(zone: &str, leader: NodeId, voters: &[NodeId], zones: &BTreeMap<NodeId, String>, current: impl Fn(NodeId) -> bool) -> Option<NodeId>`. It returns the lowest-id voter that is in `zone`, is not the leader and is current. It returns `None` when the leader is in `zone`.

- [ ] **Step 1: Write the failing unit tests** in `crates/lotse-server/src/tests.rs`:
  - `a_leader_names_a_current_voter_in_the_zone`
  - `a_leader_names_no_lagging_voter`: one that is heard but caught up longer ago than `follower_max_lag`, and one that is not heard.
  - `a_leader_names_no_voter_outside_the_zone`
  - `a_leader_in_the_zone_names_no_other_replica`
  - `a_fetch_without_zone_gets_no_preferred_replica`: served end to end through the node's fetch path.
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement it.**
  - Each round, for every group the node leads, record the time at which each voter's `matched` reached the leader's commit index.
  - "Current" means `heard_recently`, and that time lies within `follower_max_lag` of now.
  - The fetch path sets `preferred_replica` on each partition it answers as leader for a request with `zone`.
- [ ] **Step 4: Verify.** `cargo test -p lotse-server --lib` passes.
- [ ] **Step 5: Mutation-check.** Make `preferred_replica` ignore "current"; `a_leader_names_no_lagging_voter` must fail. Restore it.
- [ ] **Step 6: Commit** with "Name a caught-up replica in the reader's zone when the leader serves a fetch".

### Task 4: Server: zone reads from applied state

**Files:**
- Modify:
  - `crates/lotse-server/src/client.rs`: decode with `decode_request_tagged`, and hand `read_at_least` to the handlers.
  - `crates/lotse-server/src/client/admin.rs`: next to `metadata_leader`.
  - The handlers of the zone reads in `client/admin.rs`, `client/nodes.rs`, `client/tokens.rs`, `client/certs.rs`, `client/upgrade.rs` and `client/groups.rs` (`offset_fetch`).
  - `crates/lotse-server/src/tests.rs`

**Interfaces:**
- Consumes: `FrameTags`, `GroupIndex` (Task 1); `ClusterView.lagging` (exists).
- Produces: `fn read_replica<'a, R, S>(view: &ClusterView<'_>, groups: &'a mut Groups<R, S>, group: GroupId, at_least: Option<u64>) -> Result<&'a mut Replica<R, S>, (ErrorCode, Option<LeaderHint>)>`.
  - With `at_least == None`, it is exactly today's leader check: `metadata_leader`, or the coordinator check.
  - With `Some(n)`, it serves on the leader. On a follower it serves when `applied() >= n` and `group` is not in `view.lagging`. Otherwise it gives `NOT_LEADER` with the hint.

  Every zone-read handler calls it in place of its leader check, and reads its answer from the replica's applied state.

- [ ] **Step 1: Write the failing unit tests:**
  - `a_follower_serves_a_read_at_or_below_its_applied_index`: `DescribeConfigs` with `read_at_least = [{1, applied}]` on a follower gives `NONE` and the same configs as the leader.
  - `a_follower_refuses_a_read_above_its_applied_index`: `applied + 1` gives `NOT_LEADER` with the leader hint.
  - `a_lagging_follower_refuses_a_read`
  - `a_read_at_zero_is_served_by_a_current_follower`
  - `a_read_without_the_tag_still_needs_the_leader`
  - `offset_fetch_on_a_coordinator_voter_serves_its_applied_offsets`
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement `read_replica` and switch the ten zone reads to it.** Access checks stay as they are: they read the replica's applied role bindings.
- [ ] **Step 4: Verify.** `cargo test -p lotse-server --lib` passes.
- [ ] **Step 5: Mutation-check twice.**
  - Drop the `applied() >= n` check: `a_follower_refuses_a_read_above_its_applied_index` must fail.
  - Drop the lag check: `a_lagging_follower_refuses_a_read` must fail.
  - Restore both.
- [ ] **Step 6: Commit** with "Serve offset and admin reads from a follower that has applied what the client saw".

### Task 5: Server: `applied` on answers and coordinator voters

**Files:**
- Modify:
  - `crates/lotse-server/src/client.rs` (`Outbox`), and the reply sites of metadata-group and coordinator-group writes (`propose_admin`'s replies, the coordinator's replies in `client/coord.rs` and `client/groups.rs`).
  - `FindCoordinator` serving, and `crates/lotse-server/src/tests.rs`.

**Interfaces:**
- Consumes: `encode_response_tagged` (Task 1).
- Produces: `Outbox::push_applied(&self, id: u32, response: &Response, applied: &[GroupIndex])`.
  - Every answer a node gives from a replica of group 1 or 2 carries that replica's applied index for that group. That covers writes after they apply, leader reads, follower zone reads and `Metadata`.
  - `FindCoordinator` answers with the coordinator group's voters.

- [ ] **Step 1: Write the failing unit tests:**
  - `a_metadata_write_answer_carries_the_index_it_applied_at`: after `CreateStream`, the response's `applied` contains `{1, i}` with `i >=` the index of the create entry.
  - `a_heartbeat_answer_carries_the_coordinator_index`: after a member commits offsets, the next heartbeat answer to any member carries `{2, i}` with `i >=` the commit's index.
  - `metadata_carries_applied`
  - `find_coordinator_names_the_voters`
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement it.** Reply sites that answer after an entry applies pass the replica's applied index at that moment. Any index at or above the entry's is correct.
- [ ] **Step 4: Verify.** `cargo test -p lotse-server --lib` passes.
- [ ] **Step 5: Mutation-check.** Remove `applied` from coordinator answers: `a_heartbeat_answer_carries_the_coordinator_index` must fail. Restore it.
- [ ] **Step 6: Commit** with "Tell clients the index behind every answer from the metadata and coordinator groups".

### Task 6: Rust client: indexes, zone reads, back to the zone

**Files:**
- Modify:
  - `crates/lotse-client/src/lib.rs`: the connection decodes `applied`, and `call` encodes `read_at_least`; plus `learn`, `replica_in_zone`, the fetch path near line 1125 and the `ListOffsets` path near line 1209, and `call_coordinator`.
  - `crates/lotse-client/src/admin.rs`: `call_admin`, plus a new `call_admin_read`.
  - `crates/lotse-client/src/group.rs`: `OffsetFetch`.
  - `crates/lotse-client/src/tests.rs`
- Create: `crates/lotse-client/src/zone.rs` (the per-group indexes and the choice of a zone node; `lib.rs` is already long).

**Interfaces:**
- Consumes: Tasks 1 and 3–5.
- Produces:
  - `pub(crate) struct Indexes`: an `Arc<Mutex<BTreeMap<u64, u64>>>`, shared by the client and its connections. It has `raise(&[GroupIndex])` and `snapshot() -> Vec<GroupIndex>`.
  - `async fn call_admin_read(&self, req: &Request) -> Result<Response, Attempt>`: to the lowest-id broker whose `rack` equals `cfg.zone`, with `read_at_least`. On `NOT_LEADER`, another error answer, or a failed call, that one read goes through `call_admin`.
  - `async fn call_coordinator_read(&self, key: &str, req: &Request) -> Result<Response, Attempt>`: the same, over the coordinator voters the last `FindCoordinator` named.

- [ ] **Step 1: Write the failing tests** with scripted nodes, like the existing ADR 0104 tests:
  - `applied_raises_the_index_and_never_lowers_it`
  - `an_admin_read_goes_to_the_zone_with_read_at_least`
  - `a_refused_zone_read_goes_to_the_leader_and_the_next_tries_the_zone_again`
  - `an_older_zone_node_sends_the_read_to_the_leader`: the zone node answers `NOT_LEADER` and ignores the tag.
  - `offset_fetch_goes_to_a_coordinator_voter_in_the_zone`
  - `a_fetch_to_the_leader_names_the_zone`
  - `preferred_replica_sends_the_next_fetch_back_to_the_zone`: after a fallback, a leader answer with `preferred_replica` makes the next `Fetch` and `ListOffsets` go to that node with `allow_follower`, without a `Metadata` request.
  - `no_zone_no_tags`: a client without a zone sends neither tag, and every read goes to the leader.
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement it.**
  - Every admin `Describe*` call in the zone-reads list uses `call_admin_read`.
  - `OffsetFetch` uses `call_coordinator_read`.
  - On `preferred_replica`, clear the partition from `not_in_zone` and record the named node as its zone replica.
- [ ] **Step 4: Verify.** `cargo test -p lotse-client` passes, and `cargo clippy -p lotse-client --all-targets -- -D warnings` is clean.
- [ ] **Step 5: Mutation-check.** Stop clearing the fallback on `preferred_replica`: `preferred_replica_sends_the_next_fetch_back_to_the_zone` must fail. Restore it.
- [ ] **Step 6: Commit** with "Read offsets and admin state in the client's zone and return there when the leader says".

### Task 7: Client simulation: read-your-writes and the way back

**Files:**
- Modify: `crates/lotse-server/tests/clients.rs`

**Interfaces:**
- Consumes: Tasks 3–6.

- [ ] **Step 1: Add the oracles to the zone runs** (the half of the runs where nodes sit in two zones). They are written in the test, not taken from the client.
  - **(a)** An admin client in a zone creates resources and alters configs while faults run. Every `DescribeConfigs` and `DescribeNodes` it makes afterwards shows every change it was told succeeded.
  - **(b)** Group members in a zone commit offsets and take over partitions under faults. The first offset a member reads for a partition it takes over is never below the last offset committed by that partition's previous owner and acknowledged as committed.
- [ ] **Step 2: Add `a_reader_returns_to_its_zone_after_the_follower_heals`.** It follows the shape of `a_reader_leaves_a_follower_that_is_cut_off` at line 606.
  - Cut the follower off, and see the reader fall back to the leader.
  - Heal the cut. Within 3 s of virtual time, the reader's fetches go to the follower again, and it sends no `Metadata` request in between. Count both through the simulated network.
- [ ] **Step 3: Run it.** `LOTSE_SIM_SEEDS=500 cargo test -p lotse-server --release --test clients` passes.
- [ ] **Step 4: Mutation-check at 500 seeds.**
  - Drop the `applied() >= n` check of Task 4: oracle (a) or (b) must fail.
  - Make Task 5 omit `applied` on heartbeats: oracle (b) must fail. If it does not, add a deterministic test instead, and say so in the commit.
  - Restore both.
- [ ] **Step 5: Verify at depth.** `LOTSE_SIM_SEEDS=5000 cargo test -p lotse-server --release --test clients` passes.
- [ ] **Step 6: Commit** with "Check read-your-writes in the zone and the way back under faults".

### Task 8: `lotse mcp --zone`

**Files:**
- Modify: `crates/lotse-node/src/main.rs` and `crates/lotse-node/src/mcp.rs` (the flag into `ClientConfig::zone`), `crates/lotse-node/tests/mcp.rs`, `docs/guide/src/user/ai-agents.md`

- [ ] **Step 1: Write the failing test.** `mcp_takes_a_zone`: `lotse mcp --zone eu-1 ...` starts and answers `cluster_overview`. A unit test checks that the parsed config has `zone == Some("eu-1")`.
- [ ] **Step 2: Implement the flag.** It is also taken from `LOTSE_ZONE` when not given, like the other `LOTSE_*` variables.
- [ ] **Step 3: Verify.** `cargo test -p lotse-node --test mcp` passes.
- [ ] **Step 4: Commit** with "Let lotse mcp read in its zone".

### Task 9: Java SDK and Kotlin layer

**Files:**
- Modify:
  - `sdk/java/src/main/java/dev/lotse/proto/Frames.java`
  - The `dev.lotse.proto` message classes for `Fetch` and `FindCoordinator`
  - `sdk/java/src/main/java/dev/lotse/net/Connection.java`
  - The `dev.lotse.client` routing: the zone replica, admin calls and `OffsetFetch`
  - The tests next to the existing ADR 0104 and 0116 tests

**Interfaces:**
- Consumes: the golden vectors from Task 1. Follows Task 6 rule for rule, with the same test names in camelCase.

- [ ] **Step 1: Make the vector tests pass** for the new samples (they fail once Task 1 has run).
- [ ] **Step 2: Write the eight Task 6 tests** against scripted nodes, and see them fail.
- [ ] **Step 3: Implement it.** The Kotlin layer needs no change beyond what it wraps, so check that `sdk/kotlin` compiles.
- [ ] **Step 4: Verify.** `cd sdk/java && ./gradlew check` and `cd sdk/kotlin && ./gradlew check` pass.
- [ ] **Step 5: Mutation-check.** Do not clear the fallback on `preferredReplica`: the matching test must fail. Restore it.
- [ ] **Step 6: Commit** with "Read in the zone and return there in the Java SDK".

### Task 10: C# SDK

**Files:**
- Modify: `sdk/csharp/src/Lotse.Client/Proto/Frames.cs`, the `Fetch` and `FindCoordinator` messages, `Net/Connection.cs`, the client routing, and the tests next to the existing zone tests.

- [ ] **Steps 1–6 as in Task 9.** Use C# names: `PreferredReplica`, `ReadAtLeast`, `Applied`. Verify with `make sdk-csharp-test`. Commit with "Read in the zone and return there in the C# SDK".

### Task 11: Node.js SDK

**Files:**
- Modify: `sdk/node/src/proto/frames.ts`, the `Fetch` and `FindCoordinator` messages, `src/net/connection.ts`, the client routing, and the tests next to the existing zone tests.

- [ ] **Steps 1–6 as in Task 9.** Use TypeScript names: `preferredReplica`, `readAtLeast`, `applied`. Verify with `make sdk-node-test`. Commit with "Read in the zone and return there in the Node.js SDK".

### Task 12: ADR, design document, status, guide

**Files:**
- Create: `docs/adr/0141-reads-in-the-client-s-zone-with-read-your-writes.md`
- Modify:
  - `docs/adr/README.md`
  - `docs/design.md`, in German: the follower-read section, the frame header in `### Frames`, and the field layouts of `Fetch` and `FindCoordinator`
  - `docs/status.md`: replace the "Raft:" bullet. The gap is closed; under "Limits by design" go read-your-writes within one client only, and `Metadata` not refused.
  - `AGENTS.md`: the current-state sentence on follower reads.
  - `docs/guide/src/user/` (the page that describes `zone`) and `docs/testing.md`
  - Delete `docs/superpowers/specs/2026-11-24-zone-reads-design.md` and this plan in the same commit, as the earlier plans were.

- [ ] **Step 1: Write ADR 0141 from the spec.**
  - Context: the status gap and ADR 0116's rejected options.
  - Decision: sections 1–3 of the spec, the `Metadata` ruling, and no new MCP tools beyond `--zone`.
  - Consequences: the tests and mutation checks that actually ran, with their results.
  - Status: "Accepted (refines ADR 0104 and ADR 0116)".
- [ ] **Step 2: Update the other documents** listed above.
- [ ] **Step 3: Verify.** `mdbook build docs/guide` succeeds, and `grep -n "returns to its zone only at its next metadata refresh" -r docs AGENTS.md` finds nothing.
- [ ] **Step 4: Commit** with "Record reads in the client's zone with read-your-writes (ADR 0141)".

### Task 13: Whole-branch verification and pull request

- [ ] **Step 1: Run the checks.**
  - `cargo fmt --all`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
  - `make sdk-test` with `LOTSE_BIN` set to the built `lotse`
  - `LOTSE_SIM_SEEDS=5000 cargo test -p lotse-server --release --test clients`
- [ ] **Step 2: Open the pull request** through `gh api repos/pierrejochem/lotse/pulls`, with no AI attribution.
- [ ] **Step 3: Run the release workflow on the branch** (`gh api -X POST repos/pierrejochem/lotse/actions/workflows/release.yml/dispatches -f ref=zone-reads`). Merge only once the five CI jobs and the release run are green.
