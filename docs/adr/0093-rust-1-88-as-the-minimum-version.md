# 93. Rust 1.88 as the minimum version

- Status: Accepted
- Date: 2026-11-10

## Context
The workspace declared `rust-version = "1.85"`, the first release with edition 2024. With resolver 3, Cargo picks only dependency versions that build on that release, and two dependencies had moved past it:

- `time` 0.3.47 fixes a stack-exhaustion denial of service, and every release from 0.3.47 on needs Rust 1.88. The workspace stayed on 0.3.45, and the README listed this as a weakness.
- `sysinfo` was pinned at 0.36, the newest release that built on 1.85 (ADR 0036). Dependabot updates beyond it could not be taken.

CI, the release builds and the image build all use the current stable Rust, so the declared minimum only constrains which dependency versions Cargo chooses and which compilers are promised to work.

## Decision
`rust-version` is 1.88 for the whole workspace (`Cargo.toml`, inherited by every crate). `time` moves to 0.3.55, the newest release, which needs exactly 1.88. `sysinfo` moves to 0.38, the newest release that builds on 1.88; 0.39 and later need 1.95.

The minimum is raised again when a security fix or a needed dependency requires it, with an ADR each time, not to follow stable.

## Consequences
The `time` fix is in, and the weakness is gone from the README. Builders on Rust 1.85 to 1.87 need a newer compiler. CI does not build with exactly 1.88, so code that needs a newer compiler would be noticed only by someone building with 1.88; Cargo's resolver keeps dependencies within it.
