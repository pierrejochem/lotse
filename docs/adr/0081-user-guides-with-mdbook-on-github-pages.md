# 81. User guides with mdBook on GitHub Pages

- Status: Accepted
- Date: 2026-11-08

## Context
Operating knowledge sat in the README, the ADRs, the German design document and `sdk/PROTOCOL.md`. Nobody could follow one path from installing a node to a first message, and nothing was published for search engines or newcomers. The guides need a place that is built from the repository, so they cannot drift from the code, and a toolchain the project already has.

## Decision
The guides live in `docs/guide/` as Markdown and are built with mdBook. A workflow builds them on every pull request that touches them and deploys the result to GitHub Pages from `main`. Four guides: setup, administration, development, use. They are written in English; the design document stays German and is linked, not copied.

Examples that could drift are included, not pasted: the cluster file example, the console example and the SDK examples (which `check` compiles) come in with mdBook's `{{#include}}`, and `sdk/PROTOCOL.md` is the developer guide's protocol chapter. Where the repository documents no procedure (backup, upgrade), the guide says so instead of inventing one.

mdBook was chosen over MkDocs Material and Jekyll: it is a single Rust binary, so CI needs neither Python nor Node, and it has search and navigation built in.

## Consequences
A broken include or link fails the pull request. Config keys and commands in the guides must be checked against `crates/lotse-node` and the CLI when they are written. The site is a second place to keep current beside the README; the README keeps the short form and links to the guides.
