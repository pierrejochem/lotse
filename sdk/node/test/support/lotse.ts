import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { accessSync, constants, existsSync, mkdtempSync, openSync, readFileSync, writeFileSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { TlsConfig } from "../../src/net/tls.js";
import { address, type Address } from "../../src/net/address.js";

/** The `lotse` executable the tests that need a real node or real certificates use, or `undefined`. */
export function findBinary(): string | undefined {
  const env = process.env["LOTSE_BIN"];
  const candidates =
    env !== undefined && env.trim() !== ""
      ? [env]
      : [
          join(process.env["CARGO_TARGET_DIR"] ?? "/tmp/cargo-target", "debug", "lotse"),
          resolve(import.meta.dirname, "../../../../../../target/debug/lotse"),
        ];
  for (const c of candidates) {
    try {
      accessSync(c, constants.X_OK);
      return c;
    } catch {
      // not there
    }
  }
  return undefined;
}

/** A reason to pass to `{ skip }`, or `false` when the executable exists. */
export function skipWithoutBinary(): string | false {
  if (findBinary() !== undefined) {
    return false;
  }
  return "SKIPPED: no lotse executable. Build one with `cargo build -p lotse-node` and set LOTSE_BIN (default: /tmp/cargo-target/debug/lotse).";
}

export function binary(): string {
  const bin = findBinary();
  if (bin === undefined) {
    throw new Error("no lotse executable");
  }
  return bin;
}

/** Runs the executable and returns its output; throws on a non-zero exit. */
export function run(bin: string, ...args: string[]): string {
  return execFileSync(bin, args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 60_000 });
}

export async function freePort(): Promise<number> {
  return new Promise((resolvePort, reject) => {
    const s = net.createServer();
    s.once("error", reject);
    s.listen(0, "127.0.0.1", () => {
      const port = (s.address() as net.AddressInfo).port;
      s.close(() => resolvePort(port));
    });
  });
}

/** A one-node cluster file with TLS, certificates made by the real `lotse certs`, and a client certificate for alice. */
export class Certs {
  readonly dir: string;
  readonly config: string;
  readonly ca: string;
  readonly clientCert: string;
  readonly clientKey: string;
  readonly nodeCert: string;
  readonly nodeKey: string;

  private constructor(
    dir: string,
    readonly peerPort: number,
    readonly clientPort: number,
  ) {
    this.dir = dir;
    this.config = join(dir, "cluster.toml");
    this.ca = join(dir, "tls/ca.pem");
    this.nodeCert = join(dir, "tls/node-1.pem");
    this.nodeKey = join(dir, "tls/node-1.key");
    this.clientCert = join(dir, "clients/alice.pem");
    this.clientKey = join(dir, "clients/alice.key");
  }

  /** @param extra more lines for the `[auth]` section, such as `max_session_s = 3` */
  static async make(bin: string, extra = ""): Promise<Certs> {
    const dir = mkdtempSync(join(tmpdir(), "lotse-sdk-node-certs-"));
    const certs = new Certs(dir, await freePort(), await freePort());
    writeFileSync(
      certs.config,
      `[cluster]\ndata_dir = "${dir}/data/node-{id}"\n\n` +
        `[tls]\nca = "${dir}/tls/ca.pem"\ncert = "${dir}/tls/node-{id}.pem"\nkey = "${dir}/tls/node-{id}.key"\n\n` +
        `[retention]\nms = -1\n\n[auth]\nadmins = ["alice"]\n${extra}\n` +
        `[[node]]\nid = 1\npeer = "127.0.0.1:${certs.peerPort}"\nclient = "127.0.0.1:${certs.clientPort}"\n`,
    );
    run(bin, "certs", "--config", certs.config);
    run(bin, "certs", "--config", certs.config, "--client", "alice", "--out", join(dir, "clients"));
    return certs;
  }

  get address(): Address {
    return address("127.0.0.1", this.clientPort);
  }

  adminTls(): Promise<TlsConfig> {
    return TlsConfig.fromFiles(this.ca, this.clientCert, this.clientKey);
  }

  anonymousTls(): Promise<TlsConfig> {
    return TlsConfig.fromFiles(this.ca);
  }
}

/** A running `lotse` node, from a cluster file. */
export class NodeProcess {
  private constructor(
    private readonly child: ChildProcess,
    readonly log: string,
  ) {}

  static start(bin: string, certs: Pick<Certs, "dir" | "config">, args: string[] = []): NodeProcess {
    const log = join(certs.dir, "node.log");
    const fd = openSync(log, "a");
    const child = spawn(bin, ["run", "--config", certs.config, "--node", "1", ...args], {
      stdio: ["ignore", fd, fd],
    });
    return new NodeProcess(child, log);
  }

  async stop(): Promise<void> {
    if (this.child.exitCode !== null || this.child.signalCode !== null) {
      return;
    }
    const exited = new Promise<void>((r) => this.child.once("exit", () => r()));
    this.child.kill("SIGTERM");
    const timedOut = await Promise.race([exited.then(() => false), sleep(10_000).then(() => true)]);
    if (timedOut) {
      this.child.kill("SIGKILL");
      await exited;
    }
  }

  logText(): string {
    return existsSync(this.log) ? readFileSync(this.log, "utf8") : "";
  }
}

/** Retries `action` until it succeeds, for up to 60 s; the message names `what` and the node's log. */
export async function retry<T>(what: string, node: NodeProcess | undefined, action: () => Promise<T>): Promise<T> {
  const deadline = Date.now() + 60_000;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      return await action();
    } catch (e) {
      last = e;
      await sleep(200);
    }
  }
  throw new Error(`timed out waiting for ${what}: ${String(last)}\nnode log:\n${node?.logText() ?? ""}`, { cause: last });
}

/** Waits until `condition` holds, for up to `ms`. */
export async function eventually(what: string, condition: () => boolean, ms = 5_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (condition()) {
      return;
    }
    await sleep(10);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/** A one-node cluster file with plain TCP on loopback and no authentication, for development nodes. */
export async function makePlainCluster(): Promise<{ dir: string; config: string; address: Address }> {
  const dir = mkdtempSync(join(tmpdir(), "lotse-sdk-node-plain-"));
  const peer = await freePort();
  const client = await freePort();
  const config = join(dir, "cluster.toml");
  writeFileSync(
    config,
    `[cluster]\ndata_dir = "${dir}/data/node-{id}"\n\n[tls]\nplaintext_on_loopback = true\n\n` +
      `[retention]\nms = -1\n\n[auth]\nrequired = false\n\n` +
      `[[node]]\nid = 1\npeer = "127.0.0.1:${peer}"\nclient = "127.0.0.1:${client}"\n`,
  );
  return { dir, config, address: address("127.0.0.1", client) };
}
