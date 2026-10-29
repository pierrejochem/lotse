# 42. The console keeps its session key

- Status: Accepted
- Date: 2026-10-29

## Context

Since ADR 0041 sessions are sealed into the browser's cookie, but without `auth.session_key_file` the console made a new key at every start, so a restart still signed everyone out. Making a key by hand was one more step before the first start, and easy to forget.

## Decision

**The console makes the key once and keeps it** (maintainer's choice). The key lives in `auth.session_key_file`, by default `session.key` in the console's data directory (`data_dir`, by default `data` next to the config file; a relative path is relative to the config file). If the file does not exist, the console creates it (32 random bytes in base64, readable by its own user only: mode 0600), logs that it did, and uses it from then on. It never writes over an existing file: creating is exclusive, so a key made at the first start, or by another console starting at the same time, is the one used.

## Consequences

- Restarts keep users signed in without any setup. Consoles that share sessions behind one address need the same key file: copy it, or point them at shared storage.
- The console writes a secret to its data directory; whoever can read the file can forge sessions for the identities in them as long as the provider renews their refresh tokens. Deleting the file signs everyone out at the next start.
- A unit test checks that the file is made once with mode 0600 and that a second start reads the same key. Writing over an existing key and a readable mode were each broken on purpose and the test failed.
