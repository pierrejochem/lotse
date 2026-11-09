import { type ClientOptions, TlsConfig, scramCredentials } from "../src/index.js";

/**
 * The examples' connection settings, from the environment:
 *
 * - `LOTSE_BOOTSTRAP`: brokers, `host:port` separated by commas (default `127.0.0.1:9092`);
 * - `LOTSE_CA`, and optionally `LOTSE_CERT` and `LOTSE_KEY`: PEM files for TLS. Without `LOTSE_CA` the examples use
 *   plain TCP, which the client allows on loopback only;
 * - `LOTSE_USER` and `LOTSE_PASSWORD`: a SCRAM-SHA-512 login, if the cluster asks for one.
 */
export async function connectionOptions(more: Partial<ClientOptions> = {}): Promise<ClientOptions> {
  const env = process.env;
  const bootstrap = (env["LOTSE_BOOTSTRAP"] ?? "127.0.0.1:9092").split(",");
  const ca = env["LOTSE_CA"];
  const cert = env["LOTSE_CERT"];
  const user = env["LOTSE_USER"];
  return {
    bootstrap,
    ...(ca === undefined ? { plaintextOnLoopback: true } : { tls: await TlsConfig.fromFiles(ca, cert, env["LOTSE_KEY"]) }),
    ...(user === undefined ? {} : { credentials: scramCredentials(user, env["LOTSE_PASSWORD"] ?? "") }),
    ...more,
  };
}

/** Runs an example's `main`: a failure is printed and ends the process with status 1. */
export function run(main: () => Promise<void>): void {
  main().catch((e: unknown) => {
    console.error(e);
    process.exitCode = 1;
  });
}

/** Java's `String.hashCode`, so that the examples put a key on the same partition as the Java ones. */
export function javaHash(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (Math.imul(h, 31) + s.charCodeAt(i)) | 0;
  }
  return h;
}
