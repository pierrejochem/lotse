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
  ) {}

  static async make(bin: string, nodes: number): Promise<PlainCluster> {
    const dir = mkdtempSync(join(tmpdir(), "lotse-sdk-node-cluster-"));
    const config = join(dir, "cluster.toml");
    const addresses: Address[] = [];
    let text = `[cluster]\ndata_dir = "${dir}/data/node-{id}"\n\n[tls]\nplaintext_on_loopback = true\n\n[retention]\nms = -1\n\n[auth]\nrequired = false\n`;
    for (let id = 1; id <= nodes; id++) {
      const peer = await freePort();
      const client = await freePort();
      addresses.push(address("127.0.0.1", client));
      text += `\n[[node]]\nid = ${id}\npeer = "127.0.0.1:${peer}"\nclient = "127.0.0.1:${client}"\n`;
    }
    writeFileSync(config, text);
    return new PlainCluster(dir, config, bin, addresses);
  }

  get bootstrap(): string[] {
    return this.clientAddresses.map(formatAddress);
  }

  address(node: number): Address {
    return this.clientAddresses[node - 1] as Address;
  }

  startAll(): void {
    for (let id = 1; id <= this.clientAddresses.length; id++) {
      this.start(id);
    }
  }

  start(id: number): void {
    const fd = openSync(join(this.dir, `node-${id}.log`), "a");
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

  async stop(): Promise<void> {
    await Promise.all([...this.processes.keys()].map((id) => this.kill(id)));
  }

  log(id: number): string {
    const f = join(this.dir, `node-${id}.log`);
    return existsSync(f) ? readFileSync(f, "utf8") : "(no log)";
  }
}
