# 132. The Rust client renews its certificate through a store on the runtime seam

- Status: Accepted
- Date: 2026-11-22
- Changes [ADR 0123](0123-client-certificates-are-renewed-over-a-connection-made-with-them.md) in part: the Rust client renews by itself too

## Context

ADR 0123 lets a client renew its certificate over a connection made with it. The Java, Kotlin, C# and Node.js clients do it by themselves when given the certificate and key files: they look at the certificate at start and every hour, renew once the window opens, write the new files and connect with them from then on. The Rust client had only the call and `renew_certificate_if_due`; a Rust application had to run its own timer, write the files and reload the runtime's TLS, or run `lotse certs --renew --if-due` from a timer.

Doing what the SDKs do needs files, a clock and a TLS reload. `lotse-client` sits above the runtime seam (ADR 0006): it may not touch files or the real clock, and it runs in the simulation. The file work and the reload belong to `lotse-rt-tokio`, which does not depend on `lotse-client`.

## Decision

- **A store on the seam.** `lotse-rt` gains `CertificateStore`, beside the other seams but not part of `Runtime`: `leaf()` gives the certificate the client connects with now (DER), and `replace(key_pem, chain_pem)` puts a renewed pair in place so that connections made from then on show it.
- **The loop in the client.** `Client::keep_certificate_renewed(store, every)` runs on the client's own runtime (`Clock`, `Spawn`):
  - It looks at once and then every `every` (`RENEW_CHECK`, an hour, as in the SDKs).
  - Each look reads the leaf and calls `renew_certificate_if_due`, so nothing is asked of the cluster before the window opens.
  - In the window it renews and hands the new key and chain to the store.
  - A failure, whether reading, renewing or storing, is kept for `CertificateRenewal::failure` and tried again after `RENEW_RETRY` (a minute) or `every`, whichever is shorter. A successful look clears it.
  - The returned `CertificateRenewal` counts renewals. Dropping it, or the client, stops the loop at its next look, and the loop does not keep the client alive while it waits.
- **Files in the Tokio runtime.** `lotse_rt_tokio::CertificateFiles` is the store for a `TokioRuntime` made with `TlsConfig::from_files`:
  - `leaf` reads the first certificate of the certificate file.
  - `replace` writes the key, then the chain, each through a file beside it that is synced and moved into place; the key is readable by its owner only.
  - It then reads the files again into a `TlsConfig` (with the revocation list, if one was given) and gives it to the runtime with `reload_tls`. Open connections keep the old certificate, which stays valid until it expires.

## Consequences

- Every client renews by itself now; status.md no longer lists the Rust client as a gap.
- The loop's timing, retry, failure report and stop run in the simulation against a scripted broker, with a store in memory; `CertificateFiles` is tested on real files and a real TLS listener, and a test against a real node renews by itself and refuses a second renewal of the new certificate on a new connection.
- A Rust application other than on Tokio can renew by itself by writing its own store.
- Unlike the SDKs' TLS from files, a `TokioRuntime` does not notice files changed by someone else; it takes up what its own store wrote, and `reload_tls` is still the way for anything else.
- Rejected: the loop in `lotse-rt-tokio`. It would need a dependency on `lotse-client`, and the loop could not run in the simulation.
- Rejected: only a TLS configuration that reads changed files at the next connection, with the timer left to the application or a system timer. That is smaller, but the Rust client would still not renew by itself.
