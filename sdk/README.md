# Lotse client SDKs

Client libraries for the Lotse native protocol. The Rust code is the authority: [`crates/lotse-proto`](../crates/lotse-proto) and [`crates/lotse-record`](../crates/lotse-record) define every byte, and [`lotse-client`](../crates/lotse-client) defines the client's behaviour. The reasons are in [ADR 0056](../docs/adr/0056-client-sdks-for-the-jvm.md).

| Path | What |
| --- | --- |
| [`PROTOCOL.md`](PROTOCOL.md) | The protocol as an implementer needs it: byte layouts, the traps, what the vectors pin down |
| [`conformance/`](conformance/) | Golden vectors, written by Rust and read by every SDK |
| [`java/`](java/) | Java 17, no runtime dependencies, Gradle |

## Status

Done in the Java client: `dev.lotse.proto` (field codec, frames, every client-facing request and response, error codes), `dev.lotse.record` (record batch, builder, in-place edits, control batches), `dev.lotse.net` (connections, TLS 1.3, pipelining, pushes, session renewal) and `dev.lotse.auth` (SCRAM-SHA-512, OAUTHBEARER, the `AlterUser` credential). The client itself (streams, queues, groups, exchanges, admin) and the Kotlin layer follow.

### Connecting

```java
var tls = TlsConfig.fromFiles(Path.of("ca.pem"), Path.of("alice.pem"), Path.of("alice.key")); // certificate login
var options = ConnectionOptions.builder().tls(tls).build();
try (var connection = Connection.open(Address.parse("127.0.0.1:9092"), options)) {
    var versions = connection.apiVersions().get();                 // optional; the Rust client skips it
    var metadata = connection.call(new Request.Metadata(List.of()), Response.Metadata.class);
}
```

- TLS 1.3 only, the cluster's CA only, the certificate checked against the host you dial (IP addresses against the certificate's IP entries). `lotse certs` writes ECDSA P-256 keys in PKCS#8 PEM, which `Pem` reads; other PKCS#8 RSA and EC keys load too.
- Nothing connects until the options say `tls(...)` or `plaintextOnLoopback()`. Plain TCP to an address that is not loopback is refused before a byte is sent.
- `credentials(Credentials.scram(user, password))` or `Credentials.oauthBearer(supplier)` log in on connecting. The supplier is called for every authentication, renewals included. Use a `ConnectionPool` to have sessions renewed at four fifths of their lifetime and replaced when they end.
- Failures: `ConnectionException` (`requestSent()` tells whether the outcome is unknown), `RequestTimeoutException`, `BrokerException` (the node refused the request as a whole), `AuthenticationException`, `InsecureTransportException`.
- Listeners for pushes run on the connection's reader thread: hand work to another thread. A write to a node that has stopped reading can block the writing thread until the operating system gives up; there is no write timeout in `Socket`.

## Build and test

```sh
make sdk-test            # check the vectors against Rust, then run the Java tests
LOTSE_BIN=/path/to/lotse make sdk-test   # also the tests that need real certificates and a real node
make sdk-vectors         # regenerate sdk/conformance after changing the protocol
```

`make sdk-test` needs a JDK 17 or newer. The TLS and real-node tests use the `lotse` executable (`cargo build -p lotse-node`): they look at `LOTSE_BIN`, then at `$CARGO_TARGET_DIR/debug/lotse` (default `/tmp/cargo-target`), and skip with a message when there is none. The build targets Java 17 whatever JDK runs it. Tests ran on JDK 25 and 21.

## Conformance

`make sdk-vectors-check` regenerates the vectors into a temporary directory and compares them with `sdk/conformance`; a change to a message layout fails it until the vectors are regenerated and committed. Each SDK decodes every vector and writes it again to identical bytes.
