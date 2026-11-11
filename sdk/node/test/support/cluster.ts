import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdtempSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { address, type Address, formatAddress } from "../../src/net/address.js";
import { freePort } from "./lotse.js";

/** Real `lotse` processes from a generated cluster file: plain TCP on loopback and no authentication. */
export class PlainCluster {
  private readonly processes = new Map<number, ChildProcess>();
  private constructor(
    readonly dir: string,
    readonly config: string,
    private readonly bin: string,
    readonly clientAddresses: readonly Address[],
    private readonly attempt = 1,
  ) {}

  /**
   * A cluster of `nodes` nodes, every one of them running. Test files run in parallel, each choosing free ports, and
   * two of them can choose the same port before either node binds it: a node that finds its port taken exits at once.
   * Then the whole cluster starts again on new ports and a new data directory, up to five times.
   */
  static async started(bin: string, nodes: number): Promise<PlainCluster> {
    const dir = mkdtempSync(join(tmpdir(), "lotse-sdk-node-cluster-"));
    for (let attempt = 1; ; attempt++) {
      const cluster = await PlainCluster.make(dir, attempt, bin, nodes);
      const exited = await cluster.startAll();
      if (exited.length === 0) {
        return cluster;
      }
      await cluster.stop();
      if (attempt >= 5) {
        throw new Error(`nodes ${exited.join(", ")} exit at start\n${exited.map((id) => cluster.log(id)).join("\n")}`);
      }
    }
  }

  private static async make(dir: string, attempt: number, bin: string, nodes: number): Promise<PlainCluster> {
    const config = join(dir, `cluster-${attempt}.toml`);
    const addresses: Address[] = [];
    let text = `[cluster]\ndata_dir = "${dir}/data-${attempt}/node-{id}"\n\n[tls]\nplaintext_on_loopback = true\n\n[retention]\nms = -1\n\n[auth]\nrequired = false\n`;
    for (let id = 1; id <= nodes; id++) {
      const peer = await freePort();
      const client = await freePort();
      addresses.push(address("127.0.0.1", client));
      text += `\n[[node]]\nid = ${id}\npeer = "127.0.0.1:${peer}"\nclient = "127.0.0.1:${client}"\n`;
    }
    writeFileSync(config, text);
    return new PlainCluster(dir, config, bin, addresses, attempt);
  }

  get bootstrap(): string[] {
    return this.clientAddresses.map(formatAddress);
  }

  address(node: number): Address {
    return this.clientAddresses[node - 1] as Address;
  }

  /** Starts every node and returns those that exited within 1.5 s (none, normally). */
  private async startAll(): Promise<number[]> {
    const ids = this.clientAddresses.map((_, i) => i + 1);
    for (const id of ids) {
      this.start(id);
    }
    await sleep(1_500);
    return ids.filter((id) => {
      const p = this.processes.get(id);
      return p === undefined || p.exitCode !== null || p.signalCode !== null;
    });
  }

  start(id: number): void {
    const fd = openSync(this.logFile(id), "a");
    this.processes.set(id, spawn(this.bin, ["run", "--config", this.config, "--node", String(id)], { stdio: ["ignore", fd, fd] }));
  }

  /**
   * Starts a node again after it was killed. A port the node listened on may have been taken meanwhile (the ephemeral
   * range is shared with every outgoing connection on the machine): then the node exits at once, and this tries again.
   */
  async restart(id: number): Promise<void> {
    for (let attempt = 0; attempt < 20; attempt++) {
      this.start(id);
      const p = this.processes.get(id) as ChildProcess;
      const exited = await Promise.race([new Promise<boolean>((r) => p.once("exit", () => r(true))), sleep(1_500).then(() => false)]);
      if (!exited) {
        return;
      }
      this.processes.delete(id);
      await sleep(300);
    }
    throw new Error(`node ${id} does not start again\n${this.log(id)}`);
  }

  /** Kills a node without warning (SIGKILL), as a crash. */
  async kill(id: number): Promise<void> {
    const p = this.processes.get(id);
    if (p === undefined || p.exitCode !== null || p.signalCode !== null) {
      return;
    }
    const exited = new Promise<void>((r) => p.once("exit", () => r()));
    p.kill("SIGKILL");
    await Promise.race([exited, sleep(10_000)]);
    this.processes.delete(id);
  }

  /** Stops a node's process without ending it (SIGSTOP): it keeps its sockets and answers nothing until `resume`. */
  pause(id: number): void {
    this.processes.get(id)?.kill("SIGSTOP");
  }

  /** Lets a paused node go on (SIGCONT). */
  resume(id: number): void {
    this.processes.get(id)?.kill("SIGCONT");
  }

  async stop(): Promise<void> {
    await Promise.all([...this.processes.keys()].map((id) => this.kill(id)));
  }

  private logFile(id: number): string {
    return join(this.dir, `node-${id}-${this.attempt}.log`);
  }

  log(id: number): string {
    const f = this.logFile(id);
    return existsSync(f) ? readFileSync(f, "utf8") : "(no log)";
  }
}
