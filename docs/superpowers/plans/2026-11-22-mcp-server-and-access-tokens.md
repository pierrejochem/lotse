# `lotse mcp` and Access Tokens Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Access tokens with fixed lifetimes as a cluster credential (cluster, Rust client, console, SDKs), and a `lotse mcp` command that lets AI agents use a cluster as the signed-in user over stdio or Streamable HTTP.

**Architecture:** Tokens live in the metadata group (only a SHA-256 of the secret), are signed in with a new SASL mechanism `LOTSE-TOKEN`, and are managed through three admin requests. `lotse mcp` is a new crate `lotse-mcp` whose tools sit on `lotse-client` (generic over the `lotse-rt` traits, so they run in the simulation), with stdio and Streamable HTTP transports from `rmcp` in the `lotse` binary.

**Tech Stack:** Rust (workspace crates), `rmcp` 3.10 (official MCP SDK), axum/Tokio for HTTP, React 19 + TypeScript for the console, Java 17/Kotlin, .NET 10, Node.js 20+ for the SDKs.

**Spec:** `docs/superpowers/specs/2026-11-22-mcp-server-and-access-tokens-design.md`

## Global Constraints

- Lifetimes: exactly 1 week (7 days), 1 month (30 days), 3 months (90 days), 6 months (180 days), 1 year (365 days); request field `lifetime` 1 to 5 in that order; anything else `INVALID_REQUEST`.
- Token text: `lotse_tok_<id>_<secret>`, `<id>` 16 lowercase hex digits, `<secret>` 43 characters base64url (32 random bytes, no padding).
- The cluster stores only SHA-256 of the 32 secret bytes; the secret is drawn by the client that asks (`fill_secret`) and shown once.
- Numbers: request keys 66 `CreateToken`, 67 `DescribeTokens`, 68 `RevokeToken`; metadata change tags 35 `CreateToken`, 36 `RevokeToken`; metadata snapshot section 19; `session_mechanism::TOKEN = 4`; SASL mechanism name `LOTSE-TOKEN`.
- Limits: 50 live tokens per owner, 10,000 in all; name at most 64 bytes.
- Release 0.1.0 introduces the token format: written only while the cluster version is at least 0.1, read either way; tested with 0.0 in force (ADR 0127).
- One failure message for every failed token sign-in: `authentication failed`.
- A session signed in with a token gets `UNAUTHORIZED` for `CreateToken`.
- Rules 1, 4, 5, 7 and 8 of AGENTS.md hold; every safety rule here is mutation-checked.
- MCP: protocol 2026-07-28 (and 2025-11-25 where `rmcp` serves it); HTTP endpoint `POST /mcp`; plain HTTP only on loopback; client cache 15 minutes idle, at most 1,000; destructive tools need `confirm` equal to the resource name; `read_stream` at most 100 records or 1 MiB.
- Commits: author Pierre Jochem, no attribution trailers; one pull request per phase, merged only with the five CI jobs green and, for code, the release workflow green on the branch.
- Brand voice in UI copy and docs: calm, exact, units named, no exclamation marks.

## Review Focus

1. A token pasted with surrounding whitespace or a trailing newline (from an environment variable or the console's copy button) signs in: parsing trims ASCII whitespace (test in Task 2).
2. A SCRAM user deleted with `AlterUser` keeps no live tokens: applying the delete drops the tokens that user owns (test in Task 4).
3. Many HTTP requests at once with the same credential sign in once and share one client (test in Task 16).
4. A record whose key or value is not UTF-8 comes back from `read_stream` as base64 with `encoding: "base64"`, never as broken text (test in Task 14).
5. A token whose expiry passes while its connection is idle ends that session at the expiry, not at the next request (test in Task 6).

---

## Phase 1: tokens in the cluster and the Rust client (pull request 1)

### Task 1: Release 0.1.0 and the token format's cluster version

**Files:**
- Modify: `Cargo.toml` (workspace `version = "0.1.0"`), `crates/lotse-server/src/version.rs`, `crates/lotse-server/src/metadata.rs`
- Test: `crates/lotse-server/src/version/tests.rs`

**Interfaces:**
- Produces: `pub const TOKENS_SINCE: u32 = cluster_version::of(0, 1);` in `version.rs`; `MetadataState::writes(&self, since: u32) -> bool` (true when `cluster_version` is `Some(v)` with `v >= since`).

- [ ] **Step 1: Write the failing tests** `the_token_format_waits_for_cluster_version_0_1`: `writes(TOKENS_SINCE)` is false for `cluster_version` `None` and `Some(of(0,0))`, true for `Some(of(0,1))`; `OLDEST_CLUSTER_VERSION == of(0,0)` and `CLUSTER_VERSION == of(0,1)`.
- [ ] **Step 2: Run** `cargo test -p lotse-server --lib version` — expect FAIL (no `TOKENS_SINCE`).
- [ ] **Step 3: Implement** the constant, `writes`, and the version bump; fix anything that hard-codes `0.0.1` (`grep -rn '0\.0\.1' --include=*.rs --include=*.toml --include=*.sh --include=*.yaml`).
- [ ] **Step 4: Run** `cargo test --workspace` — expect PASS.
- [ ] **Step 5: Commit** "Raise the release to 0.1.0, which introduces the token format".

### Task 2: Token text, hash and lifetimes in `lotse-auth`

**Files:**
- Create: `crates/lotse-auth/src/token.rs`, `crates/lotse-auth/src/token/tests.rs`
- Modify: `crates/lotse-auth/src/lib.rs` (`pub mod token;`)

**Interfaces:**
- Produces:
  - `pub const MECHANISM: &str = "LOTSE-TOKEN";`
  - `pub enum Lifetime { Week = 1, Month = 2, Quarter = 3, Half = 4, Year = 5 }` with `fn from_code(u8) -> Option<Lifetime>`, `fn millis(self) -> u64`, `fn code(self) -> u8`
  - `pub struct Token { pub id: u64, pub secret: [u8; 32] }` with `fn new(id: u64, secret: [u8; 32]) -> Token`, `fn text(&self) -> String`, `fn parse(text: &str) -> Option<Token>` (trims ASCII whitespace), `fn hash(&self) -> [u8; 32]` (SHA-256 of the secret)
  - `pub fn matches(stored: &[u8; 32], presented: &Token) -> bool` (constant-time compare of hashes)

- [ ] **Step 1: Write the failing tests**: `lifetimes_are_the_five_steps` (`from_code(1..=5)` millis 7, 30, 90, 180, 365 days; `from_code(0)` and `from_code(6)` `None`); `a_token_round_trips_through_its_text` (`parse(&t.text()) == Some(t)`, text starts `lotse_tok_`, id part 16 hex, secret part 43 chars); `a_pasted_token_with_whitespace_parses` (`parse(&format!("  {}\n", t.text()))`); `malformed_tokens_do_not_parse` (wrong prefix, 15 hex digits, 42-char secret, non-base64 secret, upper-case hex); `only_the_right_secret_matches`.
- [ ] **Step 2: Run** `cargo test -p lotse-auth token` — expect FAIL.
- [ ] **Step 3: Implement** with `sha2` and the base64 crate already in the workspace (check `[workspace.dependencies]`; add `subtle` only if no constant-time compare exists there, through the workspace).
- [ ] **Step 4: Run** the tests — expect PASS.
- [ ] **Step 5: Commit** "Add access token text, hash and lifetimes".

### Task 3: Protocol messages

**Files:**
- Modify: `crates/lotse-proto/src/lib.rs` (keys, `Request`/`Response` variants, encode/decode, `ApiVersions` ranges), `crates/lotse-proto/src/messages.rs`, `crates/lotse-proto/examples/vectors.rs`
- Regenerate: `sdk/conformance/` (`make sdk-vectors`)
- Test: `crates/lotse-proto/src/tests.rs`

**Interfaces:**
- Produces:
  - `ApiKey::CREATE_TOKEN = 66`, `DESCRIBE_TOKENS = 67`, `REVOKE_TOKEN = 68`
  - `CreateTokenRequest { name: String, owner: String, lifetime: u8, secret_hash: Bytes }` → `CreateTokenResponse { error, error_msg: Option<String>, leader_hint: Option<LeaderHint>, id: u64, expires_at_ms: u64 }`
  - `DescribeTokensRequest { owner: Option<String> }` → `DescribeTokensResponse { error, leader_hint, tokens: Vec<TokenEntry> }`, `TokenEntry { id: u64, name: String, owner: String, created_at_ms: u64, expires_at_ms: u64, created_by: String }`
  - `RevokeTokenRequest { id: u64 }` → `OutcomeResponse`
  - `session_mechanism::TOKEN = 4`; `SessionEntry` gains tag `token_id: Option<u64>` (absent for other mechanisms)

- [ ] **Step 1: Write the failing tests** `token_messages_round_trip` for each request and response (all fields set, and `owner: None`), and `a_session_names_its_token`.
- [ ] **Step 2: Run** `cargo test -p lotse-proto` — expect FAIL.
- [ ] **Step 3: Implement** following the existing codec conventions (look at `RenewCertificateRequest`/`Response` and the tagged fields of `SessionEntry`); add samples to `examples/vectors.rs`.
- [ ] **Step 4: Run** `cargo test -p lotse-proto && make sdk-vectors && cargo clippy -p lotse-proto --all-targets --features samples -- -D warnings` — expect PASS and new files under `sdk/conformance`.
- [ ] **Step 5: Commit** "Add the token requests to the native protocol".

### Task 4: Tokens in the metadata state

**Files:**
- Modify: `crates/lotse-server/src/metadata.rs`
- Test: `crates/lotse-server/src/metadata/tests.rs` (or the existing tests module of `metadata.rs`)

**Interfaces:**
- Consumes: `lotse_auth::token::Lifetime`, `version::TOKENS_SINCE`, `MetadataState::writes`.
- Produces:
  - `Change::CreateToken { id: u64, name: String, owner: String, hash: [u8; 32], created_at_ms: u64, expires_at_ms: u64, created_by: String }` (tag 35; `id` is 0 in the proposal and taken as the change's index when applied, as resource ids are)
  - `Change::RevokeToken { id: u64 }` (tag 36)
  - `MetadataState::tokens: BTreeMap<u64, StoredToken>`, `StoredToken { name, owner, hash, created_at_ms, expires_at_ms, created_by }`
  - `MetadataState::token_room(&self, owner: &str, now_ms: u64) -> Result<(), String>` (50 per owner, 10,000 in all, live ones only)
  - `MetadataState::live_token(&self, id: u64, now_ms: u64) -> Option<&StoredToken>`
  - pub consts `MAX_TOKENS_PER_OWNER = 50`, `MAX_TOKENS = 10_000`

- [ ] **Step 1: Write the failing tests**: `a_created_token_is_kept_and_revoked` (apply create, `live_token` finds it, apply revoke, gone); `expired_tokens_go_when_the_next_token_change_applies`; `room_for_tokens_is_limited_per_owner_and_in_all`; `tokens_survive_a_snapshot` (section 19 written and read); `a_snapshot_without_section_19_reads_with_no_tokens`; `deleting_a_user_drops_their_tokens` (Review Focus 2: apply the existing user-delete change for `alice`, her tokens are gone, Bob's stay); `token_changes_round_trip_through_their_record`.
- [ ] **Step 2: Run** `cargo test -p lotse-server --lib metadata` — expect FAIL.
- [ ] **Step 3: Implement**: encode/decode the two changes beside tags 33 and 34; the snapshot writes section 19 only when `writes(TOKENS_SINCE)` holds (the section count stays 18 otherwise); the user-delete apply path also removes that owner's tokens.
- [ ] **Step 4: Run** the tests — expect PASS.
- [ ] **Step 5: Commit** "Keep access tokens in the metadata state".

### Task 5: Admin requests for tokens

**Files:**
- Create: `crates/lotse-server/src/client/tokens.rs`
- Modify: `crates/lotse-server/src/client/mod.rs` (dispatch keys 66 to 68), `crates/lotse-server/src/access.rs` (who may), `crates/lotse-server/src/client/stats.rs` (`lotse_tokens`)
- Test: `crates/lotse-server/tests/auth.rs`

**Interfaces:**
- Consumes: Task 3 messages, Task 4 changes, `Clients::metadata_leader`, `propose_admin` (as `revoke_sessions` in `client/admin.rs` does).
- Produces: `Clients::create_token`, `Clients::describe_tokens`, `Clients::revoke_token` (each `(&mut self, outbox, id, req, session, view, groups)` in the style of `revoke_sessions`).
- Rules: owner = the session's principal unless the session is a cluster admin, who may name anyone; a token session may not create (`UNAUTHORIZED`, message "a session signed in with a token cannot create tokens"); lifetime code checked; name 1 to 64 bytes; `secret_hash` exactly 32 bytes; `token_room` checked at the leader; before `writes(TOKENS_SINCE)`: `UNSUPPORTED_VERSION` with "tokens need cluster version 0.1; raise it with `lotse upgrade finalize`"; describe: own tokens for anyone, all for cluster admin and cluster viewer, filtered by `owner` if given; revoke: own token, or any for a cluster admin; expiry = leader's `unix_ms` + lifetime.

- [ ] **Step 1: Write the failing simulation tests** in `tests/auth.rs`: `users_create_list_and_revoke_their_own_tokens`; `a_cluster_admin_creates_a_token_for_another_principal_and_others_cannot`; `token_lifetimes_are_only_the_five_steps` (code 0 and 6 refused, 1 to 5 give expiries 7/30/90/180/365 days after creation); `token_limits_hold` (the 51st for one owner refused with a message naming 50); `tokens_wait_for_cluster_version_0_1` (with 0.0 in force: refused, nothing written).
- [ ] **Step 2: Run** `cargo test -p lotse-server --test auth tokens` — expect FAIL.
- [ ] **Step 3: Implement** `client/tokens.rs` and the dispatch and access entries.
- [ ] **Step 4: Run** the tests — expect PASS.
- [ ] **Step 5: Commit** "Serve CreateToken, DescribeTokens and RevokeToken".

### Task 6: Signing in with a token

**Files:**
- Modify: `crates/lotse-server/src/client/session.rs` (handshake and `authenticate` arm), `crates/lotse-server/src/auth.rs` (`mechanisms()` offers `LOTSE-TOKEN` when `tokens` is on; `AuthConfig::tokens: bool`, default true), `crates/lotse-server/src/client/sessions.rs` (token id in `DescribeSessions`), `crates/lotse-node/src/config.rs` (`[auth] tokens`, default true), `crates/lotse-server/src/client/stats.rs` (`lotse_token_sign_ins_total{result}`)
- Test: `crates/lotse-server/tests/auth.rs`

**Interfaces:**
- Consumes: `lotse_auth::token::{Token, matches, MECHANISM}`, `MetadataState::live_token`.
- Produces: a session established with mechanism `TOKEN`, principal = owner, no groups, `left` = expiry minus now, credential time = token creation (so `RevokeSessions` for the owner covers tokens made before it); the session records `token_id` so a revoke or expiry closes it.

- [ ] **Step 1: Write the failing simulation tests**: `a_token_signs_in_as_its_owner_with_only_name_roles`; `every_failed_token_sign_in_says_the_same` (unknown id, wrong secret, expired, revoked: all `authentication failed`); `revoking_a_token_ends_its_connections_at_once`; `an_idle_session_ends_when_its_token_expires` (Review Focus 5: no request after sign-in; the connection is closed within a second of expiry); `a_token_session_cannot_create_tokens`; `revoking_the_owners_sessions_ends_token_sessions`; `describe_sessions_names_the_token_id_and_never_the_secret`.
- [ ] **Step 2: Run** `cargo test -p lotse-server --test auth token` — expect FAIL.
- [ ] **Step 3: Implement** the mechanism arm beside OAUTHBEARER; closing on revoke reuses the revocation signal (`revocations_changed`) that ends sessions of revoked certificates.
- [ ] **Step 4: Run** the tests — expect PASS.
- [ ] **Step 5: Mutation-check**: one at a time, break (a) the hash compare (accept any secret), (b) the expiry check, (c) the revoke closing, (d) the token-session `CreateToken` refusal, (e) the lifetime step check, (f) the per-owner limit; each must fail a test from Task 5 or 6. Restore each.
- [ ] **Step 6: Commit** "Sign in with an access token".

### Task 7: Tokens in the Rust client

**Files:**
- Modify: `crates/lotse-client/src/auth.rs` (`Credentials::Token(String)`, `Debug` hides it), `crates/lotse-client/src/lib.rs` (SASL with `LOTSE-TOKEN`), `crates/lotse-client/src/admin.rs`
- Test: `crates/lotse-client/src/tests.rs` (scripted broker) and `crates/lotse-server/tests/auth.rs`

**Interfaces:**
- Produces:
  - `Credentials::Token(String)`
  - `Client::create_token(&self, name: &str, owner: Option<&str>, lifetime: Lifetime) -> Result<CreatedToken, ClientError>`, `CreatedToken { id: u64, text: String, expires_at_ms: u64 }` (draws the secret with `fill_secret`, sends only the hash)
  - `Client::describe_tokens(&self, owner: Option<&str>) -> Result<Vec<lotse_proto::TokenEntry>, ClientError>`
  - `Client::revoke_token(&self, id: u64) -> Result<(), ClientError>`

- [ ] **Step 1: Write the failing tests**: `create_token_sends_only_the_hash` (scripted broker records the request: 32-byte hash equals SHA-256 of the returned text's secret, the secret bytes appear nowhere in the frame); `token_credentials_are_not_printed` (`format!("{:?}", Credentials::Token(t))` has no secret); and in `tests/auth.rs` `a_client_signs_in_with_a_token_it_created` end to end.
- [ ] **Step 2: Run** — expect FAIL.
- [ ] **Step 3: Implement**.
- [ ] **Step 4: Run** `cargo test -p lotse-client && cargo test -p lotse-server --test auth token` — expect PASS.
- [ ] **Step 5: Commit** "Use and manage access tokens from the Rust client".

### Task 8: Docs and the pull request for phase 1

**Files:**
- Create: `docs/adr/0133-access-tokens-with-fixed-lifetimes.md`
- Modify: `docs/adr/README.md`, `docs/design.md` (German), `docs/status.md`, `AGENTS.md` (current state, protocol keys), `docs/guide/src/admin/` (a tokens section beside certificates), `sdk/PROTOCOL.md` (mechanism and keys 66 to 68)

- [ ] **Step 1: Write** ADR 0133 from the spec's token section (context, decision, consequences, rejected: tokens with groups, free lifetimes, secret drawn by the cluster).
- [ ] **Step 2: Run** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace --no-fail-fast` — expect PASS.
- [ ] **Step 3: Commit** "Document access tokens", push, open the pull request, dispatch the release workflow on the branch; merge when the five CI jobs and the release run are green.

## Phase 2: the console (pull request 2)

### Task 9: Console backend

**Files:**
- Modify: `crates/lotse-console/src/cluster/mod.rs` (trait), `cluster/native.rs`, `cluster/demo.rs`, `crates/lotse-console/src/api.rs`, `crates/lotse-console/src/lib.rs` (routes), `crates/lotse-console/src/cli.rs` (command hints)
- Regenerate: `console/web/openapi.json` and the TS client (`npm run api`)
- Test: `crates/lotse-console/tests/native.rs`, the api tests next to `api.rs`

**Interfaces:**
- Produces: `ClusterApi::tokens(&self, who: &Identity, owner: Option<&str>) -> ApiResult<TokensView>`, `create_token(&self, who, req: CreateTokenBody) -> ApiResult<CreatedTokenView>`, `revoke_token(&self, who, id: u64) -> ApiResult<()>`; routes `GET /api/v1/clusters/{cluster}/tokens?owner=`, `POST /api/v1/clusters/{cluster}/tokens` (`{name, lifetime: "1w"|"1m"|"3m"|"6m"|"1y", owner?}`), `POST /api/v1/clusters/{cluster}/tokens/{id}/revoke`. `CreatedTokenView { id, text, expires_at }` is the only place the text appears; it is never logged.

- [ ] **Step 1: Write the failing tests**: `the_console_creates_lists_and_revokes_tokens_as_the_user` against real nodes; `a_created_token_text_is_returned_once_and_never_listed`; `a_user_cannot_list_another_users_tokens`.
- [ ] **Step 2: Run** `cargo test -p lotse-console` — expect FAIL.
- [ ] **Step 3: Implement**, each action through the signed-in user's client (rule 8); demo cluster keeps tokens in memory.
- [ ] **Step 4: Run** the tests and `cd console/web && npm run api` — expect PASS and no OpenAPI drift.
- [ ] **Step 5: Commit** "Manage access tokens from the console backend".

### Task 10: The *Tokens* tab

**Files:**
- Create: `console/web/src/pages/Tokens.tsx`, `console/web/src/pages/Tokens.test.tsx`
- Modify: `console/web/src/pages/UsersAccess.tsx` (a `tokens` tab after *Sessions*)

**Interfaces:**
- Consumes: the generated client calls for the three routes of Task 9.

- [ ] **Step 1: Write the failing tests**: the table lists name, owner, created, expires, last four characters of the id; *Create token* offers exactly the five lifetimes with *1 month* selected; after creating, the dialog shows the text once with "This token is shown once. Store it now; it cannot be shown again." and a copy button, and closing it leaves the text nowhere in the page; revoking asks for confirmation naming that connections signed in with the token end at once; the IdP-group note appears in the dialog; an admin sees an owner filter and an owner field, others do not.
- [ ] **Step 2: Run** `cd console/web && npm test` — expect FAIL.
- [ ] **Step 3: Implement** with the existing Radix dialog and table patterns and the brand tokens.
- [ ] **Step 4: Run** `npm run typecheck && npm test && npm run build` — expect PASS.
- [ ] **Step 5: Commit** "Add the Tokens tab to the console"; update `docs/guide` console pages and `docs/console/README.md` screenshots list; push, pull request, CI; merge when green (no release run needed for a console-only change).

## Phase 3: the SDKs (pull request 3)

### Task 11: Java SDK and Kotlin layer

**Files:**
- Modify: `sdk/java/src/main/java/dev/lotse/proto/` (the three messages, mechanism 4), `dev/lotse/auth/` (token text, hash), `dev/lotse/client/Credentials.java`, `LotseClient.java` (`createToken`, `describeTokens`, `revokeToken`), `sdk/kotlin/src/main/kotlin/dev/lotse/kotlin/` (suspend wrappers)
- Test: the vector tests, unit tests beside each, the real-node tests (`RealCluster`)

**Interfaces:**
- Produces: `Credentials.token(String)`, `CreatedToken createToken(String name, String owner /* nullable */, TokenLifetime lifetime)`, `List<TokenEntry> describeTokens(String owner)`, `void revokeToken(long id)`, enum `TokenLifetime { WEEK, MONTH, QUARTER, HALF_YEAR, YEAR }`; Kotlin `suspend fun createToken(...)`.

- [ ] **Step 1: Write the failing tests**: vectors for the three messages decode and encode byte for byte; `tokenTextRoundTripsAndTrims`; `createTokenSendsOnlyTheHash`; real node: create, sign in with the token, revoke, sign-in fails.
- [ ] **Step 2: Run** `make sdk-test` with `LOTSE_BIN` — expect FAIL.
- [ ] **Step 3: Implement** rule for rule as the Rust client in Task 7.
- [ ] **Step 4: Run** `make sdk-test` — expect PASS; mutation-check the hash-only rule and the trim.
- [ ] **Step 5: Commit** "Use and manage access tokens from the Java SDK and Kotlin layer".

### Task 12: C# SDK

Same as Task 11 in `sdk/csharp` (`Lotse.Proto`, `Lotse.Auth`, `Lotse.Client`): `Credentials.Token(string)`, `CreateTokenAsync`, `DescribeTokensAsync`, `RevokeTokenAsync`, `TokenLifetime`. Run `make sdk-csharp-test`. Commit "Use and manage access tokens from the C# SDK".

### Task 13: Node.js SDK

Same as Task 11 in `sdk/node` (`src/proto`, `src/auth`, `src/client`): `{ token: string }` credentials, `createToken`, `describeTokens`, `revokeToken`, `TokenLifetime`. Run `make sdk-node-test` (Node.js 20 and 24). Then update the four SDK guide pages, push, pull request, CI; merge when green. Commit "Use and manage access tokens from the Node.js SDK".

## Phase 4: `lotse mcp` (pull request 4)

### Task 14: The `lotse-mcp` crate and its tools

**Files:**
- Create: `crates/lotse-mcp/Cargo.toml`, `src/lib.rs` (the tool registry), `src/look.rs`, `src/data.rs`, `src/change.rs`, `src/destroy.rs`, `src/tokens.rs`, `src/tests.rs`
- Modify: workspace `Cargo.toml` (member, `[workspace.dependencies]` for `serde_json` if not there; no `rmcp` here)
- Test: `crates/lotse-mcp/tests/tools.rs` (simulation against real nodes)

**Interfaces:**
- Consumes: `lotse_client::Client<R>` and its calls (metadata, `node_metrics`, `describe_group` or the group-lag call, `fetch`/read, `produce`, `publish`, `list_offsets`, admin calls, `node_sessions`, `describe_upgrade`, `describe_tokens`).
- Produces:
  - `pub struct Tools<R> { client: Arc<Client<R>> }` with `pub fn list() -> Vec<ToolSpec>` (deterministic order) and `pub async fn call(&self, name: &str, args: serde_json::Value) -> ToolResult`
  - `pub struct ToolSpec { name: &'static str, description: &'static str, input_schema: serde_json::Value, destructive: bool, read_only: bool }`
  - `pub struct ToolResult { structured: serde_json::Value, text: String, is_error: bool }`
  - The tool names of the spec's table, exactly: `cluster_overview`, `list_resources`, `describe_stream`, `describe_queue`, `describe_consumer_group`, `node_metrics`, `list_sessions`, `describe_upgrade`, `read_stream`, `produce`, `publish`, `list_offsets`, `create_stream`, `create_queue`, `declare_exchange`, `bind`, `unbind`, `alter_configs`, `create_partitions`, `delete_resource`, `purge_queue`, `requeue_dead_letters`, `list_tokens`.

- [ ] **Step 1: Write the failing tests**: `tools_are_listed_in_a_fixed_order_with_schemas`; `a_viewer_can_look_but_not_produce` (the produce result has `is_error` and the broker's `UNAUTHORIZED` message); `read_stream_stops_at_100_records_or_1_mib`; `binary_keys_and_values_come_back_as_base64` (Review Focus 4); `destructive_tools_need_the_name_to_confirm` (missing or wrong `confirm`: `is_error`, nothing deleted; right name: deleted); `invalid_arguments_are_tool_errors_not_panics`.
- [ ] **Step 2: Run** `cargo test -p lotse-mcp` — expect FAIL.
- [ ] **Step 3: Implement** each tool as one or a few client calls; no state between calls.
- [ ] **Step 4: Run** — expect PASS; mutation-check the `confirm` rule and the read limits.
- [ ] **Step 5: Commit** "Add the lotse-mcp tools".

### Task 15: `lotse mcp` over stdio

**Files:**
- Create: `crates/lotse-node/src/mcp.rs` (transport glue: `rmcp` server handler delegating to `Tools`)
- Modify: `crates/lotse-node/src/main.rs` (subcommand `mcp`), `crates/lotse-node/Cargo.toml` (`lotse-mcp`, `rmcp` with server, stdio and streamable-HTTP-server features, through the workspace)
- Test: `crates/lotse-node/tests/mcp.rs`

**Interfaces:**
- Consumes: `lotse_mcp::Tools`, `lotse_node::admin_client`-style construction from `--config` or `--bootstrap` + `--ca`.
- Produces: `lotse mcp [--config <file> | --bootstrap <host:port,...> --ca <file>] [--cert <file> --key <file>]`; credentials from `LOTSE_TOKEN`, or `LOTSE_USER` + `LOTSE_PASSWORD`, or the certificate; none: exit 2 with a message naming the three ways.

- [ ] **Step 1: Write the failing tests** against a real node started from a cluster file: `stdio_answers_discover_and_lists_the_tools` (spawn the executable, send `server/discover` and `tools/list` as JSON-RPC lines, read the answers); `stdio_calls_a_tool_as_the_token_owner`; `stdio_refuses_to_start_without_a_credential`; nothing but JSON-RPC on stdout (logs only on stderr).
- [ ] **Step 2: Run** `cargo test -p lotse-node --test mcp stdio` — expect FAIL.
- [ ] **Step 3: Implement**.
- [ ] **Step 4: Run** — expect PASS.
- [ ] **Step 5: Commit** "Run lotse mcp over stdio".

### Task 16: `lotse mcp` over Streamable HTTP

**Files:**
- Create: `crates/lotse-node/src/mcp/http.rs` (listener, auth, client cache), `crates/lotse-node/src/mcp/cache.rs`
- Test: `crates/lotse-node/tests/mcp.rs`

**Interfaces:**
- Produces: `lotse mcp ... --listen <host:port> [--tls-cert <file> --tls-key <file>] [--allow-origin <origin>]...`; `ClientCache::get_or_sign_in(&self, credential: &HttpCredential) -> Result<Arc<Client<TokioRuntime>>, SignInError>` keyed by SHA-256 of the credential, 15 minutes idle, 1,000 at most (least recently used out), sign-ins for one key at a time.

- [ ] **Step 1: Write the failing tests**: `http_needs_a_credential` (no header: 401 with `WWW-Authenticate`, no tool runs); `http_bearer_token_and_basic_password_both_work`; `a_revoked_token_fails_on_its_next_request`; `concurrent_requests_with_one_credential_sign_in_once` (Review Focus 3: 20 requests at once, one sign-in counted by the node's `lotse_token_sign_ins_total`); `plain_http_is_refused_off_loopback` (`--listen 0.0.0.0:…` without TLS exits with a message); `a_browser_origin_is_refused_unless_allowed`; `https_with_the_given_certificate`.
- [ ] **Step 2: Run** `cargo test -p lotse-node --test mcp http` — expect FAIL.
- [ ] **Step 3: Implement** with `rmcp`'s Streamable HTTP service behind an axum layer that checks `Origin` and `Authorization` before any MCP handling.
- [ ] **Step 4: Run** — expect PASS; mutation-check the 401 path, the loopback rule and the per-credential client separation (a request must never reach another credential's client).
- [ ] **Step 5: Commit** "Run lotse mcp over Streamable HTTP".

### Task 17: Docs, the AGENTS.md rule, and the pull request for phase 4

**Files:**
- Create: `docs/adr/0134-lotse-mcp-a-separate-command-for-ai-agents.md`, `docs/guide/src/use/ai-agents.md` (add to `SUMMARY.md`)
- Modify: `docs/adr/README.md`, `docs/design.md` (German), `docs/status.md` (gaps: no OAuth flow; no tool takes queue messages), `AGENTS.md` (repository map: `crates/lotse-mcp`, `lotse mcp`; the convention below; current state), `packaging/` man or help text if the binary's help lists subcommands

- [ ] **Step 1: Add to AGENTS.md *Conventions***: "**MCP follows the features.** The plan for a new feature or a changed request includes its tools in `lotse mcp` (`crates/lotse-mcp`): what an operator or a developer would ask an agent to do with it, each as the signed-in principal. They ship in the same change, with tests; a feature without tools says why in its ADR."
- [ ] **Step 2: Write** ADR 0134 and the guide page (stdio setup for a desktop agent with `LOTSE_TOKEN`, the shared HTTP mode with TLS and Bearer or Basic, the tool list).
- [ ] **Step 3: Run** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace --no-fail-fast && mdbook build docs/guide` — expect PASS.
- [ ] **Step 4: Commit** "Document lotse mcp and make MCP tools part of every feature", push, pull request, release workflow on the branch; merge when green.
