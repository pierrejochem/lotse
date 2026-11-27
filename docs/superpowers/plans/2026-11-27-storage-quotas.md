# Storage Quotas Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Storage limits per tenant and per name prefix, which refuse new data with `QUOTA_EXCEEDED` once reached. Segments also close by age, so a tenant that drains its queues frees space. Both ship in release 0.2.0.

**Architecture:**
- **Segments close by age:** a replica closes its active segment by age on its retention pass, which already runs every second. A per-resource `segment.ms` overrides the node default.
- **Counting:** the metadata leader adds up usage per limit after each size poll: group size times the number of listed voters. It records only full and cleared transitions in the metadata state, as it does a low disk (ADR 0122/0124).
- **Enforcement:** group leaders refuse new data through the existing `Admission` gate.
- **Limits:** set with `AlterQuota` (key 69) and read with `DescribeQuotas` (key 70).

**Tech Stack:** Rust workspace (`lotse-log`, `lotse-proto`, `lotse-server`, `lotse-client`, `lotse-mcp`, `lotse-node`, `lotse-console`), React 19 + TypeScript (console), Java 17/Kotlin, .NET 10, Node.js 20+ (SDKs).

**Spec:** `docs/superpowers/specs/2026-11-27-storage-quotas-design.md`

## Global Constraints

- **Release 0.2.0:** `CLUSTER_VERSION` 0.2, `OLDEST_CLUSTER_VERSION` 0.1. `QUOTAS_SINCE = SEGMENT_AGE_SINCE = cluster_version::of(0, 2)`. New formats are written only while `MetadataState::writes(<since>)` and read either way. Each format has a test with 0.1 in force.
- **Numbers:**
  - request keys: 69 `AlterQuota`, 70 `DescribeQuotas`;
  - metadata change tags: 37 `SetQuota`, 38 `QuotaFull`;
  - metadata snapshot section: 20;
  - error `QUOTA_EXCEEDED` = 10, not retriable.
- **Limits:**
  - bytes at least 1; absent removes the limit;
  - at most 100 prefix limits per tenant;
  - a limit becomes full when usage ≥ limit, and clears when usage < 95 % of the limit (`usage * 100 < limit * 95`, in integers);
  - a limit below current usage is allowed.
- **Usage:** Σ over covered groups of (largest reported log size × number of voters the metadata state lists).
  - Covered by a tenant limit: all the tenant's runtime streams and queues.
  - Covered by a prefix limit: those whose name starts with the prefix (byte prefix, as role bindings).
  - The metadata and coordinator groups are never counted.
  - A group no replica reported keeps its last known size on this leader.
  - A leader proposes nothing until every runtime group has a known size.
- **Access:**
  - tenant limit (empty prefix): cluster admin;
  - prefix limit: cluster admin, or tenant admin in that tenant whose binding prefix covers the limit's prefix;
  - a prefix limit set by a cluster admin: cluster admin only;
  - `DescribeQuotas`: cluster admin and cluster viewer see all, others see the tenants they hold a role in;
  - an unknown or invisible tenant: `UNKNOWN_RESOURCE`.
- **Refused when full:** new data batches (after the duplicate check): produces (transactional too), a node's writes for `Publish`, forwarded dead letters (they stay owed in their source). Everything else passes.
- **Settings:**
  - `[quotas] check_interval_ms`: 1,000 to 600,000, default 10,000. It replaces `SIZES_EVERY_TICKS`, and simulations keep 1,000 ticks.
  - `[cluster] segment_ms`: default 3,600,000.
  - `segment.ms` per runtime stream or queue: at least 60,000.
- **Console:** a warning from 80 % of a limit, a full state at 100 %.
- **Rules:** AGENTS.md rules 1 to 8 hold. Every safety rule here is mutation-checked (rule 5). Formats follow the cluster version. MCP follows the features.
- **Commits:** `git -c user.name="Pierre Jochem" -c user.email="pierrejochem@msn.com" commit`, no attribution trailers. One pull request per phase, merged only with the five CI jobs green and the release workflow green on the branch.
- **Brand voice** in UI copy and docs: calm, exact, units named, no exclamation marks. `docs/design.md` stays German.

## Review Focus

1. **A tenant deleted and created again** starts with no limits and no full record from its earlier incarnation (test in Task 7).
2. **An idempotent producer refused with `QUOTA_EXCEEDED`** sends the same batch again after the limit cleared, and it is taken with the same sequence: the sequence stayed free (test in Task 10).
3. **A transactional producer that meets `QUOTA_EXCEEDED`** can still abort, because its markers pass the gate. A full tenant never leaves a transaction holding back the last stable offset (test in Task 10).
4. **A publish to an exchange bound to two queues**, one under a full prefix and one not, writes the other queue and reports the refused one (test in Task 10).
5. **A stream that gets no new data** after its segment closed by age can still be cut by retention, because a boundary is noted at the new segment's base (test in Task 3).

---

## Phase 1: release 0.2.0 and segments that close by age (pull request 1)

### Task 1: Release 0.2.0

**Files:**
- Modify: `Cargo.toml` (workspace `version = "0.2.0"`), `Cargo.lock`, `packaging/helm/lotse/Chart.yaml` (`version`, `appVersion`)
- Modify: every SDK version that `packaging/sdk-versions.sh` checks (run it to list them)
- Modify: `crates/lotse-server/src/version.rs`, `crates/lotse-server/src/version/tests.rs`
- Create: `docs/releases/0.2.0.md`

**Interfaces:**
- Produces: `pub const QUOTAS_SINCE: u32 = cluster_version::of(0, 2);` and `pub const SEGMENT_AGE_SINCE: u32 = cluster_version::of(0, 2);` in `version.rs`

- [ ] **Step 1: Write the failing test** in `version/tests.rs`:
  - `release_0_2_runs_beside_0_1`: `CLUSTER_VERSION == of(0,2)` and `OLDEST_CLUSTER_VERSION == of(0,1)`;
  - `quota_formats_wait_for_0_2`: a state with 0.1 in force gives `!writes(QUOTAS_SINCE)` and `!writes(SEGMENT_AGE_SINCE)`; with 0.2 in force, both true. Follow the existing `TOKENS_SINCE` test.
- [ ] **Step 2:** Run `cargo test -p lotse-server version::` → FAIL (constants missing).
- [ ] **Step 3:** Raise the versions and add the two constants with doc comments citing the spec. `docs/releases/0.2.0.md` starts with a heading and two placeholder bullets, storage quotas and segments closing by age; Task 16 and Task 5 fill them.
- [ ] **Step 4:** Run `cargo test -p lotse-server version::` → PASS. Run `packaging/sdk-versions.sh 0.2.0` → every version matches. Run `cargo test --workspace` → PASS. Existing tests that pinned 0.1 as the newest version are updated to name 0.2, and the change is noted in the commit body.
- [ ] **Step 5: Commit** "Raise the release to 0.2.0, which introduces quotas and segment age".

### Task 2: `Log::roll`

**Files:**
- Modify: `crates/lotse-log/src/lib.rs`
- Test: `crates/lotse-log/src/tests.rs`

**Interfaces:**
- Produces: `pub async fn roll(&mut self) -> Result<bool, Error>` on `Log<D>`. It seals the active segment (sync, then write its index, exactly as the size roll in `append` does) and creates an empty one at `next_offset`. It returns `false` and does nothing when the active segment holds no batch.

- [ ] **Step 1: Write the failing tests:**
  - `roll_seals_the_active_segment`: append 3 batches, `roll()` returns true, `segment_count() == 2`, the last segment's `base_offset == next_offset()`, all 3 batches still readable, and reopening the log gives the same segments and offsets.
  - `roll_of_an_empty_active_segment_does_nothing`: a fresh log, or straight after a roll, returns false and `segment_count()` is unchanged.
- [ ] **Step 2:** Run `cargo test -p lotse-log roll_` → FAIL.
- [ ] **Step 3:** Implement. Move the sealing code out of `append` into a private helper that both call.
- [ ] **Step 4:** Run `cargo test -p lotse-log` → PASS.
- [ ] **Step 5: Commit** "Let a log seal its active segment on request".

### Task 3: Replicas close segments by age

**Files:**
- Modify: `crates/lotse-server/src/replica.rs`
- Modify: `crates/lotse-server/src/node.rs` (the retention pass near `group.retain_with`)
- Modify: `crates/lotse-server/src/lib.rs` (`RetentionConfig` gains `segment_ms: u64`, default 3,600,000)
- Test: the replica's unit tests (`crates/lotse-server/src/replica/tests.rs` or wherever `retain` is tested now)

**Interfaces:**
- Consumes: `Log::roll` (Task 2)
- Produces:
  - `pub fn set_segment_ms(&mut self, ms: Option<u64>)` on `Replica`; `None` never rolls by age.
  - `retain_with(now_ms, sweep)` first rolls when the active segment is non-empty and was first seen non-empty by this replica at least `ms` earlier.
  - The replica keeps `(active_base, first_seen_ms)` in memory. After a restart the age starts again at the first pass, which is conservative.
  - After a roll, when everything is applied (`applied + 1 == log.next_offset()`), it notes a boundary at the new base, so retention can cut there without new data.

- [ ] **Step 1: Write the failing tests:**
  - `a_segment_closes_by_age`: a replica with Stream retention, `set_segment_ms(Some(60_000))`. Append and apply, then call `retain(t)` at t = 0 and t = 59_999 → 1 segment; at t = 60_000 → 2 segments.
  - `an_aged_stream_is_cut_without_new_data` (Review Focus 5): Stream retention `ms = 1_000`, `segment_ms = 60_000`. Append batches with timestamp 0, call `retain(60_000)` (rolls), then `retain(61_001)`. `log().start_offset()` has moved past those batches, with no batch appended in between.
  - `no_roll_without_a_segment_age`: `set_segment_ms(None)` never rolls.
- [ ] **Step 2:** Run `cargo test -p lotse-server a_segment_closes_by_age an_aged_stream no_roll_without` → FAIL.
- [ ] **Step 3:** Implement in `Replica`, and wire `node.rs`: runtime stream and queue groups get `Some(settings or cfg.retention.segment_ms)`, the metadata and coordinator groups get `None`. Per-resource values come in Task 4; for now pass `cfg.retention.segment_ms`.
- [ ] **Step 4:** Run `cargo test -p lotse-server` → PASS.
- [ ] **Step 5: Mutation check:** make the roll never fire (`ms` treated as `None`) → `a_segment_closes_by_age` fails. Make the boundary after a roll not noted → `an_aged_stream_is_cut_without_new_data` fails. Undo both.
- [ ] **Step 6: Commit** "Close a replica's active segment once it is an hour old".

### Task 4: The `segment.ms` setting

**Files:**
- Modify: `crates/lotse-node/src/config.rs` (`[cluster] segment_ms`, at least 60,000, into `RetentionConfig.segment_ms`)
- Modify: `crates/lotse-server/src/metadata.rs` (`StreamSettings.segment_ms: Option<i64>`, `StreamChange.segment_ms`, and the queue's `segment_ms` in its settings and in `QueueChange`; encoding gated as below)
- Modify: `crates/lotse-server/src/client/admin.rs` (`const SEGMENT_MS: &str = "segment.ms";` in create, alter and describe for streams and queues)
- Modify: `crates/lotse-server/src/node.rs` (per-resource value, else the node default)
- Modify: `packaging/cluster.toml.example`

**Interfaces:**
- Consumes: `SEGMENT_AGE_SINCE` (Task 1), `Replica::set_segment_ms` (Task 3)
- Produces: config key `segment.ms` on runtime streams and queues; `DescribeConfigs` names its source (default, runtime, configuration) as for `retention.ms`

- [ ] **Step 1: Write the failing tests:**
  - in `client/tests.rs`, `segment_ms_is_set_altered_and_described`: create a stream with `segment.ms = 120000`; DescribeConfigs shows 120000 from the runtime source. Alter it back to the default; it shows 3600000 from the default. The same for a queue. `segment.ms = 59999` gives `INVALID_REQUEST`.
  - `segment_ms_waits_for_0_2`: with cluster version 0.1 in force, creating or altering with `segment.ms` gives `INVALID_REQUEST`, with a message naming 0.2. Follow how the token requests refuse before their version.
  - in `config.rs` tests: `segment_ms` below 60,000 is refused, and its default is 3,600,000.
- [ ] **Step 2:** Run `cargo test -p lotse-server segment_ms_ && cargo test -p lotse-node segment_ms` → FAIL.
- [ ] **Step 3:** Implement. A stream or queue record with `segment.ms` set is written only while `writes(SEGMENT_AGE_SINCE)`; without it, records encode as today, so 0.1 nodes still read them. Follow `StreamChange::has_compaction` for how a new field keeps older records readable.
- [ ] **Step 4:** Run `cargo test -p lotse-server && cargo test -p lotse-node` → PASS.
- [ ] **Step 5: Commit** "Set a stream's or queue's segment age with segment.ms".

### Task 5: Segment age under faults, its ADR and docs

**Files:**
- Modify: `crates/lotse-server/tests/retention.rs`
- Create: `docs/adr/0146-segments-close-by-age.md` (copy `template.md`), and its row in `docs/adr/README.md`
- Modify: `docs/design.md` (German: the log and retention sections), `docs/status.md`, `AGENTS.md` (the current-state sentence), `docs/guide/src/admin/*` (the retention page), `docs/releases/0.2.0.md`
- Modify: `crates/lotse-mcp/src/change.rs` only if `alter_configs` keeps a list of known keys (it needs no `confirm` for `segment.ms`)

- [ ] **Step 1: Write the failing test.** In the retention simulation add `drained_queues_and_aged_streams_free_their_space` (`check_few_seeds`, under the simulation's faults). A queue with `segment.ms = 60000` is produced to, then fully consumed and acked. A stream has `retention.ms = 1000` and `segment.ms = 60000`. After 3 minutes of virtual time with no new data, both have dropped their old segments on every replica: log start past the data, and size below the first batch's size.
- [ ] **Step 2:** Run `LOTSE_SIM_SEEDS=50 cargo test -p lotse-server --release --test retention drained_queues` → it fails if Task 3 or Task 4 missed a path. If it passes at once, mutate (Step 4) to show it can fail.
- [ ] **Step 3:** Fix what it finds.
- [ ] **Step 4: Mutation check:** the node passes `None` for every group → the test fails. Undo.
- [ ] **Step 5:** Write the ADR, from the spec's decision 11 and "Segments close by age"; status Accepted. Its consequences: more small files (up to 24 a day per partition), the restart rule, the `segment.ms` setting, and the mutation checks. Update the docs listed.
- [ ] **Step 6:** Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` → clean. Run `LOTSE_SIM_SEEDS=1000 cargo test -p lotse-server --release --test retention` → PASS.
- [ ] **Step 7: Commit** "Free a drained queue's space within its segment age". Open pull request 1, dispatch `release.yml` on the branch, and merge when everything is green.

---

## Phase 2: quotas in the cluster, the Rust client and `lotse mcp` (pull request 2)

### Task 6: Protocol

**Files:**
- Modify: `crates/lotse-proto/src/lib.rs` (`ApiKey::ALTER_QUOTA = 69`, `DESCRIBE_QUOTAS = 70`, `SUPPORTED` grows to 70 entries), `crates/lotse-proto/src/messages.rs`, `crates/lotse-proto/src/samples.rs`, `crates/lotse-proto/src/tests.rs`
- Regenerate: `sdk/conformance/` (`make sdk-vectors`)
- Modify: `sdk/PROTOCOL.md`, `docs/design.md` (field layouts)

**Interfaces:**
- Produces, in `lotse-proto`:
  - `AlterQuotaRequest { tenant: String, prefix: String, bytes: Option<u64> }` → `AlterQuotaResponse { error: ErrorCode, error_msg: Option<String> }`
  - `DescribeQuotasRequest { tenant: Option<String> }` → `DescribeQuotasResponse { error, error_msg, counted_at_ms: Option<u64>, quotas: Vec<QuotaEntry> }`
  - `QuotaEntry { tenant: String, prefix: String, bytes: Option<u64>, set_by_cluster_admin: bool, full: bool, used_bytes: Option<u64> }`. A tenant without a limit appears with `prefix: ""` and `bytes: None`.
  - `Request::AlterQuota`, `Request::DescribeQuotas` and the matching `Response` variants.

- [ ] **Step 1: Write the failing tests** in `lotse-proto` tests: round-trip both requests and responses through the codec, including `bytes: None`, an empty prefix and `counted_at_ms: None`; and `quota_exceeded_is_not_retriable`: `!ErrorCode::QUOTA_EXCEEDED.retriable()`.
- [ ] **Step 2:** Run `cargo test -p lotse-proto quota` → FAIL.
- [ ] **Step 3:** Implement. Add samples so that `examples/vectors.rs` writes them.
- [ ] **Step 4:** Run `cargo test -p lotse-proto`, `cargo clippy -p lotse-proto --all-targets --features samples -- -D warnings` and `make sdk-vectors` → PASS; the new vectors are committed.
- [ ] **Step 5: Commit** "Add AlterQuota and DescribeQuotas to the native protocol".

### Task 7: Limits and full records in the metadata state

**Files:**
- Modify: `crates/lotse-server/src/metadata.rs`
- Test: `crates/lotse-server/src/metadata/tests.rs`

**Interfaces:**
- Consumes: `QUOTAS_SINCE` (Task 1)
- Produces:
  - `pub(crate) struct Quota { pub(crate) bytes: u64, pub(crate) by_cluster_admin: bool, pub(crate) full: bool }`
  - `MetadataState.quotas: BTreeMap<(String, String), Quota>`, keyed by (tenant, prefix); `""` is the tenant limit.
  - `Change::SetQuota { tenant: String, prefix: String, bytes: Option<u64>, by_cluster_admin: bool }` (tag 37). `None` removes the limit. A changed limit keeps its `full` flag until the next count.
  - `Change::QuotaFull { tenant: String, prefix: String, full: bool }` (tag 38). It does nothing for a limit that no longer exists.
  - Snapshot section 20, written only when `quotas` is non-empty and `writes(QUOTAS_SINCE)`.
  - `pub(crate) const MAX_PREFIX_QUOTAS: usize = 100;`. Apply ignores a `SetQuota` that would be the 101st prefix limit of a tenant; the request checks it first, so only a race reaches apply.
  - Deleting a tenant drops its quotas.

- [ ] **Step 1: Write the failing tests:**
  - `quotas_round_trip_in_the_snapshot`;
  - `a_quota_change_reads_back`, an encode and decode test for both changes;
  - `quota_records_wait_for_0_2`: with 0.1 in force, the snapshot has 19 sections even with quotas present (follow the token test);
  - `a_deleted_tenant_drops_its_quotas` (Review Focus 1): set a limit, mark it full, delete the tenant, create it again → no quotas;
  - `the_101st_prefix_limit_is_ignored`;
  - `quota_full_for_a_removed_limit_does_nothing`.
- [ ] **Step 2:** Run `cargo test -p lotse-server metadata::tests::quota` (and the other names) → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p lotse-server metadata::` → PASS.
- [ ] **Step 5: Commit** "Keep storage limits and full limits in the metadata state".

### Task 8: Counting usage

**Files:**
- Create: `crates/lotse-server/src/quota.rs` (with `mod tests;` and `quota/tests.rs`), registered in `lib.rs`

**Interfaces:**
- Consumes: `MetadataState` (streams, queues and their voters, `quotas`), `Change::QuotaFull` (Task 7)
- Produces:
  - `pub(crate) struct Usage { pub(crate) per_limit: BTreeMap<(String, String), u64>, pub(crate) per_tenant: BTreeMap<String, u64> }`
  - `pub(crate) fn count(state: &MetadataState, sizes: &BTreeMap<GroupId, u64>) -> Option<Usage>`. It returns `None` when some runtime group of a stream or queue has no size. `per_tenant` covers every tenant with runtime resources, with or without a limit.
  - `pub(crate) fn changes(state: &MetadataState, usage: &Usage) -> Vec<Change>`. It proposes `QuotaFull { full: true }` where usage ≥ bytes and the limit is not full, and `{ full: false }` where usage × 100 < bytes × 95 and the limit is full. It proposes nothing otherwise.

- [ ] **Step 1: Write the failing tests:**
  - `every_listed_voter_counts`: a stream of 2 partitions with 3 voters each and sizes 100 and 50 → 450.
  - `a_name_counts_against_every_matching_prefix`: limits `orders.` and `orders.eu.`; the stream `orders.eu.x` counts in both and in the tenant, and the stream `ordersx` counts in neither prefix.
  - `other_tenants_and_the_cluster_groups_do_not_count`.
  - `a_group_without_a_size_means_no_count`: `count` returns `None`.
  - `full_at_the_limit_and_clear_below_95_percent`: limit 1,000. Usage 999 → nothing; 1,000 → full. Once full: 951 → nothing; 950 → nothing (950 × 100 = 95,000, not below 95,000); 949 → clear.
  - `a_limit_below_usage_is_full_at_the_next_count`.
- [ ] **Step 2:** Run `cargo test -p lotse-server quota::` → FAIL.
- [ ] **Step 3:** Implement. Iterate `BTreeMap`s only (rule 1).
- [ ] **Step 4:** Run `cargo test -p lotse-server quota::` → PASS.
- [ ] **Step 5: Mutation check:** count each group once instead of per voter → `every_listed_voter_counts` fails. Use the longest prefix only → `a_name_counts_against_every_matching_prefix` fails. Clear at 100 % → `full_at_the_limit_and_clear_below_95_percent` fails. Undo each.
- [ ] **Step 6: Commit** "Count each limit's usage from group sizes and voters".

### Task 9: The metadata leader counts and records

**Files:**
- Modify: `crates/lotse-server/src/node.rs` (`ask_for_sizes`, `Shared`, the planning step after `ForgetDeleted`, `SIZES_EVERY_TICKS` replaced by `cfg.quota_check_interval`)
- Modify: `crates/lotse-server/src/lib.rs` (`NodeConfig.quota_check_interval: Duration`; the default for `NodeConfig::new` and simulations is `tick * 1_000`)
- Modify: `crates/lotse-node/src/config.rs` (`[quotas] check_interval_ms`, 1,000 to 600,000, default 10,000), `packaging/cluster.toml.example`
- Test: `crates/lotse-server/src/node/tests.rs` or a simulation in `crates/lotse-server/tests/nodes.rs`

**Interfaces:**
- Consumes: `quota::count`, `quota::changes` (Task 8)
- Produces:
  - `Shared.sizes` merges each poll into the previous one (a group not reported keeps its size). It starts empty on a node that becomes leader, and the map is dropped when the node stops leading.
  - `Shared.quota_usage: Mutex<Option<(u64 /* counted_at_ms */, quota::Usage)>>`, set after each poll that `count` accepts and cleared when the node stops leading. Task 11 and Task 12 read it.
  - In the planning step, while caught up: propose every `quota::changes(state, usage)` with `propose_internal`.

- [ ] **Step 1: Write the failing tests** (simulated three-node cluster, deterministic):
  - `a_full_limit_is_recorded_and_cleared`: a tenant limit of 10 KiB. Produce 20 KiB → within two check intervals the metadata state has the limit full. Raise the limit to 1 MiB → full clears within two intervals.
  - `a_down_node_does_not_stop_counting`: a stopped node still leaves every group with a size from the others, so a limit still trips.
  - `a_new_metadata_leader_keeps_a_full_limit`: trip a limit, move metadata leadership with a transfer, and before the new leader's first complete count the limit is still full.
  - `check_interval_ms_is_read_and_bounded` in `config.rs` tests.
- [ ] **Step 2:** Run `cargo test -p lotse-server a_full_limit_is_recorded a_down_node_does_not_stop a_new_metadata_leader_keeps && cargo test -p lotse-node check_interval_ms` → FAIL.
- [ ] **Step 3:** Implement. The rebalancer and `ForgetDeleted` keep reading `sizes` as before; their behaviour is unchanged apart from merged sizes.
- [ ] **Step 4:** Run `cargo test -p lotse-server` → PASS. The membership simulation still passes: `LOTSE_SIM_SEEDS=200 cargo test -p lotse-server --release --test membership`.
- [ ] **Step 5: Mutation check:** treat a missing size as 0 (`count` never returns `None`) → `a_new_metadata_leader_keeps_a_full_limit` fails. Undo.
- [ ] **Step 6: Commit** "Record full storage limits from the metadata leader's count".

### Task 10: Leaders refuse new data in a full tenant or prefix

**Files:**
- Modify: `crates/lotse-server/src/client.rs` (`ClusterView::quota_full(&self, g: GroupId) -> Option<String>`, `Admission`, `ProposeError` handling → `QUOTA_EXCEEDED` with `error_msg`)
- Modify: `crates/lotse-server/src/replica.rs` (`ProposeError::QuotaExceeded(String)`)
- Modify: `crates/lotse-server/src/client/retire.rs` (dead letters: refuse with `QUOTA_EXCEEDED`, letters stay owed), `crates/lotse-server/src/client/publish.rs` (only if it does not go through `Admission`)
- Test: `crates/lotse-server/src/client/tests.rs`, `crates/lotse-server/tests/queues.rs` (dead letters)

**Interfaces:**
- Consumes: `MetadataState.quotas` (Task 7)
- Produces:
  - `quota_full(g)` finds the group's resource and tenant. It returns a message naming the tenant limit, or the first full prefix limit its name matches, when one is recorded full. For example, `the storage limit of tenant "a" is reached; nothing was written` or `the storage limit of prefix "orders." in tenant "a" is reached; nothing was written`.
  - `Admission { full, quota: Option<String>, gate }`; `check` refuses with `QuotaExceeded` after `StorageFull` and before the length gate.

- [ ] **Step 1: Write the failing tests** (unit, with a `ClusterView` whose state has a full limit):
  - `a_full_tenant_refuses_new_data`: produce → `QUOTA_EXCEEDED`, a message naming the tenant, and nothing written.
  - `a_full_prefix_refuses_only_its_names`: prefix `orders.` full; produce to `orders.a` is refused, to `billing.a` is taken.
  - `a_duplicate_of_a_taken_batch_is_still_written`: take a batch, mark the tenant full, send the same batch again → success with the original offset.
  - `a_refused_sequence_stays_free` (Review Focus 2): an idempotent batch refused while full is taken with the same sequence once the record clears.
  - `a_transaction_in_a_full_tenant_aborts` (Review Focus 3): begin a transaction and produce, then mark the tenant full; a further produce is refused, `EndTxn(abort)` succeeds, and the last stable offset passes the transaction.
  - `acks_and_reads_pass_in_a_full_tenant`.
  - `a_publish_writes_the_queue_with_room` (Review Focus 4): an exchange bound to `orders.q` (prefix full) and `billing.q`; publish → `billing.q` written, `orders.q` reported `QUOTA_EXCEEDED`.
  - in `tests/queues.rs`: `dead_letters_wait_while_the_target_is_full`. They are delivered once the record clears, never lost or doubled.
- [ ] **Step 2:** Run `cargo test -p lotse-server a_full_tenant a_full_prefix a_duplicate_of a_refused_sequence a_transaction_in acks_and_reads a_publish_writes dead_letters_wait` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p lotse-server` → PASS.
- [ ] **Step 5: Mutation check:** the gate ignores the record → `a_full_tenant_refuses_new_data` fails. Prefix limits are ignored → `a_full_prefix_refuses_only_its_names` fails. Duplicates are refused too (check before the duplicate test) → `a_duplicate_of_a_taken_batch_is_still_written` fails. Undo each.
- [ ] **Step 6: Commit** "Refuse new data once a storage limit is full".

### Task 11: `AlterQuota` and `DescribeQuotas` on a node

**Files:**
- Create: `crates/lotse-server/src/client/quotas.rs` (with tests in `client/quotas/tests.rs`)
- Modify: `crates/lotse-server/src/client.rs` (dispatch), `crates/lotse-server/src/access.rs` (rules)

**Interfaces:**
- Consumes: `Change::SetQuota` (Task 7), `Shared.quota_usage` (Task 9), `MAX_PREFIX_QUOTAS`, `QUOTAS_SINCE`
- Produces:
  - **`AlterQuota`:** on the metadata leader, once the change commits; elsewhere `NOT_LEADER`. Error cases:
    - before `writes(QUOTAS_SINCE)`: `INVALID_REQUEST`, "storage quotas need cluster version 0.2";
    - `bytes: Some(0)`: `INVALID_REQUEST`;
    - a 101st prefix limit: `INVALID_REQUEST`, "a tenant holds at most 100 prefix limits";
    - an unknown or invisible tenant: `UNKNOWN_RESOURCE`;
    - not allowed by the access rules in the Global Constraints: `UNAUTHORIZED`.
  - It writes `SetQuota { by_cluster_admin: principal is cluster admin }`.
  - **`DescribeQuotas`:** on the metadata leader, from its applied state plus `quota_usage`. `used_bytes` is `None` before the first count, and so is `counted_at_ms`. Elsewhere `NOT_LEADER`. Filtered by visibility.

- [ ] **Step 1: Write the failing tests:**
  - `a_cluster_admin_sets_and_removes_a_tenant_limit`;
  - `a_tenant_admin_cannot_set_the_tenant_limit`;
  - `a_tenant_admin_sets_a_prefix_limit_under_their_binding`: binding prefix `orders.`; the limit `orders.eu.` is allowed, `billing.` is `UNAUTHORIZED`;
  - `a_tenant_admin_cannot_change_a_cluster_admins_prefix_limit`;
  - `a_zero_limit_is_refused`;
  - `the_101st_prefix_limit_is_refused`;
  - `quotas_need_0_2`;
  - `describe_shows_only_visible_tenants`: a viewer of tenant a sees a's entries only, and a cluster viewer sees all;
  - `describe_before_the_first_count_has_no_usage`;
  - `a_follower_answers_not_leader`.
- [ ] **Step 2:** Run `cargo test -p lotse-server client::quotas::` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p lotse-server` → PASS. Add the two requests to the access simulation's model only if it enumerates every request (`crates/lotse-server/tests/access_sim.rs`); its oracle is written in the test.
- [ ] **Step 5: Mutation check:** drop the cluster-admin-set check → `a_tenant_admin_cannot_change_a_cluster_admins_prefix_limit` fails. Ignore the binding prefix → `a_tenant_admin_sets_a_prefix_limit_under_their_binding` fails. Undo.
- [ ] **Step 6: Commit** "Set and read storage limits with AlterQuota and DescribeQuotas".

### Task 12: Quotas on `/metrics`

**Files:**
- Modify: `crates/lotse-server/src/ops.rs`
- Test: its unit tests, and `crates/lotse-server/tests/metrics.rs` if OpenMetrics output is checked there

**Interfaces:**
- Produces:
  - every node: `lotse_quota_bytes{tenant,prefix}` and `lotse_quota_full{tenant,prefix}` (0 or 1), from its applied state;
  - the metadata leader also: `lotse_quota_used_bytes{tenant,prefix}`, for every limit and for every tenant with `prefix=""`, from `quota_usage`.
  - Label values are escaped as the other labels are.

- [ ] **Step 1: Write the failing test** `quota_metrics_render`: limits in the state and a usage give exactly these three families with the expected lines; a node that is not the leader renders no `used_bytes`.
- [ ] **Step 2:** Run `cargo test -p lotse-server ops::` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p lotse-server ops::` → PASS.
- [ ] **Step 5: Commit** "Report storage limits and usage on the operator's endpoint".

### Task 13: Rust client

**Files:**
- Modify: `crates/lotse-client/src/admin.rs`, `crates/lotse-client/src/lib.rs` (produce and publish error handling)
- Test: `crates/lotse-client/src/tests.rs`

**Interfaces:**
- Produces:
  - `pub async fn alter_quota(&self, tenant: &str, prefix: &str, bytes: Option<u64>) -> Result<(), ClientError>`
  - `pub async fn describe_quotas(&self, tenant: Option<&str>) -> Result<lotse_proto::DescribeQuotasResponse, ClientError>`
  - Both go to the metadata leader and retry `NOT_LEADER` as `describe_upgrade` does.
  - A produce or publish that gets `QUOTA_EXCEEDED` ends at once with the broker's error and its message. It is not retried.

- [ ] **Step 1: Write the failing tests:**
  - `quota_exceeded_ends_a_produce_at_once`: a fake broker answers `QUOTA_EXCEEDED`; `produce` returns the error after exactly one attempt and the error's text names the limit.
  - `alter_and_describe_quotas_reach_the_metadata_leader`.
- [ ] **Step 2:** Run `cargo test -p lotse-client quota` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p lotse-client` → PASS.
- [ ] **Step 5: Commit** "Add storage limits to the Rust client".

### Task 14: The quota simulation

**Files:**
- Create: `crates/lotse-server/tests/quotas.rs`
- Modify: `.github/workflows/release.yml` (a `quota-simulation` job at 5,000 seeds, like the access simulation), `AGENTS.md` and `docs/testing.md` (ten simulations)

- [ ] **Step 1: Write the simulation** `storage_limits_hold_under_faults` (`check_seeds`). Three nodes with crashes, partitions and metadata leader changes; `check_interval_ms` of 200 ticks; small segments. Tenants `a` (limit) and `b` (none), with a producer and a queue consumer in each; `a` also has a prefix limit on `a.big.`. The oracle is written in the test, from what clients saw:
  1. `b` never gets `QUOTA_EXCEEDED`.
  2. Once the test's own estimate of `a`'s usage (Σ acknowledged bytes × voters) exceeds its limit plus what `a` may write in two check intervals, a produce to `a` that is still acknowledged is a failure. The estimate counts only what clients saw acknowledged.
  3. Reads and acks in `a` succeed throughout, faults aside.
  4. After the limit is raised, produces to `a` are acknowledged again within 3 intervals of healing.
  5. The existing invariants hold: no acknowledged batch is lost, and no refused batch is in the log.
- [ ] **Step 2:** Run `LOTSE_SIM_SEEDS=200 cargo test -p lotse-server --release --test quotas` → PASS, or fix what it finds (rule 4: decide whether the code or the check is wrong, and say which in the commit).
- [ ] **Step 3: Mutation check:** the gate ignores the record → oracle 2 fails. A new metadata leader clears limits at once → the simulation fails. Undo.
- [ ] **Step 4:** Run `LOTSE_SIM_SEEDS=1000 cargo test -p lotse-server --release --test quotas` → PASS.
- [ ] **Step 5: Commit** "Check storage limits under faults in a simulation of their own".

### Task 15: `lotse mcp` tools

**Files:**
- Modify: `crates/lotse-mcp/src/look.rs` (`describe_quotas`), `crates/lotse-mcp/src/change.rs` (`set_quota`), `crates/lotse-mcp/src/lib.rs` (`Tools::list`, `Tools::call`)
- Test: `crates/lotse-mcp/tests/tools.rs`, `crates/lotse-node/tests/mcp.rs` (tool count)
- Modify: `docs/guide/src/user/ai-agents.md`

**Interfaces:**
- Produces:
  - `describe_quotas { tenant?: string }` → the entries as JSON, with `used_bytes` and `counted_at`;
  - `set_quota { tenant: string, prefix?: string, bytes: integer | null }`, with no `confirm`.
  - The tool count becomes 25.

- [ ] **Step 1: Write the failing tests:**
  - `set_quota_then_describe_quotas` against simulated nodes, as a cluster admin;
  - `set_quota_as_a_viewer_is_refused`;
  - the list test expects 25 tools.
- [ ] **Step 2:** Run `cargo test -p lotse-mcp quota` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p lotse-mcp && cargo test -p lotse-node --test mcp` → PASS.
- [ ] **Step 5: Commit** "Read and set storage limits from lotse mcp".

### Task 16: The ADR and docs for quotas

**Files:**
- Create: `docs/adr/0147-storage-quotas-per-tenant-and-prefix.md`, plus its index row; mark ADR 0011 as superseded in part
- Modify: `docs/design.md` (German: Flow Control & Quotas, the error table moving `QUOTA_EXCEEDED` from "später" to built, field layouts), `docs/status.md` (the overshoot and segment rules under "Limits by design"), `AGENTS.md` (repository map: `quota.rs`, `client/quotas.rs`, `tests/quotas.rs`; the current-state sentence), `docs/guide/src/admin/` (a page on storage quotas), `docs/releases/0.2.0.md`

- [ ] **Step 1:** Write the ADR from the spec's decisions 1 to 16, status Accepted. Its consequences: overshoot of up to two check intervals of writing, space freed within the segment age, metadata leader changes, and the mutation checks of Tasks 8 to 14.
- [ ] **Step 2:** Update the docs listed. Run `mdbook build docs/guide` → builds.
- [ ] **Step 3:** Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` → clean. Run `LOTSE_SIM_SEEDS=1000 cargo test -p lotse-server --release --test quotas --test queues --test metadata --test access_sim` → PASS.
- [ ] **Step 4: Commit** "Record the storage quota decisions". Open pull request 2, dispatch `release.yml` on the branch, and merge when everything is green.

---

## Phase 3: SDKs and the console (pull request 3)

### Task 17: Java SDK and the Kotlin layer

**Files:**
- Modify: `sdk/java/src/main/java/dev/lotse/proto/*` (messages from Task 6), `sdk/java/src/main/java/dev/lotse/client/LotseClient.java`, and the Kotlin layer's client file
- Test: the Java proto vector tests (they pick up the new vectors), plus `QuotaTest.java` and a Kotlin test

**Interfaces:**
- Produces:
  - Java: `void alterQuota(String tenant, String prefix, Long bytes)`, `DescribeQuotasResponse describeQuotas(String tenant)` (`null` for all); Kotlin: the same as `suspend`.
  - `QUOTA_EXCEEDED` is not retriable, and ends a produce after one attempt.

- [ ] **Step 1: Write the failing tests:**
  - `quotaExceededEndsAProduceAtOnce` (fake broker, as the existing routing tests do);
  - `alterAndDescribeQuotas`;
  - the vector tests decode keys 69 and 70.
- [ ] **Step 2:** `make sdk-test` cannot run here (Gradle is blocked by the proxy). Write the tests to the existing fixtures' pattern and rely on CI's sdk job, saying so in the PR.
- [ ] **Step 3:** Implement, rule for rule as the Rust client.
- [ ] **Step 4:** On CI: the sdk job passes.
- [ ] **Step 5: Commit** "Add storage limits to the Java SDK and the Kotlin layer".

### Task 18: C# SDK

**Files:**
- Modify: `sdk/csharp/src/Lotse.Proto/*`, `sdk/csharp/src/Lotse.Client/LotseClient.cs`
- Test: `sdk/csharp/tests/*` (`QuotaTests.cs`)

**Interfaces:**
- Produces: `Task AlterQuotaAsync(string tenant, string prefix, ulong? bytes)`, `Task<DescribeQuotasResponse> DescribeQuotasAsync(string? tenant)`. `QUOTA_EXCEEDED` is not retriable.

- [ ] **Step 1: Write the failing tests:** `QuotaExceededEndsAProduceAtOnce`, `AlterAndDescribeQuotas`, and the vectors.
- [ ] **Step 2:** NuGet is blocked here too, so CI verifies (the sdk job).
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** On CI: the sdk job passes.
- [ ] **Step 5: Commit** "Add storage limits to the C# SDK".

### Task 19: Node.js SDK

**Files:**
- Modify: `sdk/node/src/proto/*`, `sdk/node/src/client/client.ts`
- Test: `sdk/node/test/*`

**Interfaces:**
- Produces: `alterQuota(tenant: string, prefix: string, bytes: bigint | null): Promise<void>` and `describeQuotas(tenant?: string): Promise<DescribeQuotasResponse>`; `QUOTA_EXCEEDED` is not retriable.

- [ ] **Step 1: Write the failing tests:** "quota exceeded ends a produce at once", "alter and describe quotas", and the vectors.
- [ ] **Step 2:** Run `make sdk-node-test` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `make sdk-node-test` → PASS.
- [ ] **Step 5: Commit** "Add storage limits to the Node.js SDK".

### Task 20: Console

**Files:**
- Modify: `crates/lotse-console/src/cluster/native.rs` (fill `quota_bytes` and `used_bytes` from `describe_quotas`, plus prefix limits), `crates/lotse-console/src/cluster/mod.rs` and `cluster/demo.rs` (the same model), `crates/lotse-console/src/model.rs` (`prefix_quotas: Vec<PrefixQuota { prefix, bytes, used_bytes, full, set_by_cluster_admin }>` on the tenant), `crates/lotse-console/src/api.rs` (`PUT /api/tenants/{tenant}/quota`, and `PUT` and `DELETE /api/tenants/{tenant}/quotas/{prefix}`, as the signed-in user)
- Modify: `console/web/src/pages/Tenants.tsx`, `console/web/src/components/CreateQueueDialog.tsx` (no refusal; a note that writes are refused while full), `console/web/src/api/queries.ts`; regenerate `console/web/openapi.json` and `schema.d.ts` (`npm run api`)
- Test: `crates/lotse-console/tests/native.rs`, `console/web` tests

**Interfaces:**
- Consumes: `Client::alter_quota`, `Client::describe_quotas` (Task 13)
- Produces: tenant rows with limit, usage and state:
  - "Near limit" from 80 %;
  - "Full" at a recorded full;
  - the edit action shown only to those the cluster would allow, following `who_am_i` roles as other pages do.

- [ ] **Step 1: Write the failing tests:**
  - `native.rs`: `quotas_show_on_tenants`: set a limit through the API as a cluster admin, and the tenants endpoint returns it with usage once counted.
  - the web tests: Tenants renders the 80 % warning and the full state; CreateQueueDialog's submit is enabled in a full tenant.
- [ ] **Step 2:** Run `cargo test -p lotse-console --test native quotas_show && (cd console/web && npm test)` → FAIL.
- [ ] **Step 3:** Implement within ADR 0140 (navy primary, red only for danger, `TableWrap`) and the brand tokens.
- [ ] **Step 4:** Run `cd console/web && npm run api && npm run typecheck && npm test && npm run build`, then `cargo test -p lotse-console` → PASS. The OpenAPI file is committed.
- [ ] **Step 5: Commit** "Show and set storage limits in the console". Open pull request 3, merge when the five CI jobs and the release workflow are green, then report to the maintainer.
