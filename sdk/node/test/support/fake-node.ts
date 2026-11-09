import type { Request, Response } from "../../src/index.js";
import { type Address, formatAddress } from "../../src/net/address.js";
import { type FakeBroker, type Peer } from "./fake-broker.js";
import { FakeBroker as Broker } from "./fake-broker.js";

/** What a handler returns to close the connection without answering. */
export const DROP = Symbol("drop the connection");

/** What a handler does with a request: an answer, `null` to stay silent, or `DROP` to break the connection. */
export type NodeHandler = (
  node: FakeNode,
  request: Request,
  peer: Peer,
) => Response | null | typeof DROP | Promise<Response | null | typeof DROP>;

/** A broker with an id, in this process, that answers as a test tells it to. Plain TCP on loopback. */
export class FakeNode {
  private handler: NodeHandler = (node, request) => {
    throw new Error(`node ${node.id} was not expected to get ${request.type}`);
  };
  private readonly received: Request[] = [];
  private broker!: FakeBroker;

  private constructor(readonly id: number) {}

  static async start(id: number): Promise<FakeNode> {
    const node = new FakeNode(id);
    node.broker = await Broker.start(async (peer, req) => {
      node.received.push(req.request);
      const answer = await node.handler(node, req.request, peer);
      if (answer === null) {
        return;
      }
      if (answer === DROP) {
        peer.closeSocket();
        return;
      }
      peer.reply(req.header.correlationId, answer);
    });
    return node;
  }

  on(handler: NodeHandler): this {
    this.handler = handler;
    return this;
  }

  get address(): Address {
    return this.broker.address;
  }

  get hostPort(): string {
    return formatAddress(this.address);
  }

  get connections(): number {
    return this.broker.accepted;
  }

  /** Every request this node received, in arrival order. */
  requests(): Request[];
  requests<T extends Request["type"]>(type: T): Extract<Request, { type: T }>[];
  requests(type?: string): Request[] {
    return type === undefined ? [...this.received] : this.received.filter((r) => r.type === type);
  }

  count(type: Request["type"]): number {
    return this.requests(type).length;
  }

  /** Stops the node: it no longer accepts, and its connections are closed. */
  async close(): Promise<void> {
    await this.broker.close();
  }
}

/** Starts `n` nodes with ids 1 to `n`. */
export async function startNodes(n: number): Promise<FakeNode[]> {
  const out: FakeNode[] = [];
  for (let i = 1; i <= n; i++) {
    out.push(await FakeNode.start(i));
  }
  return out;
}

export async function closeAll(nodes: readonly FakeNode[]): Promise<void> {
  await Promise.all(nodes.map((n) => n.close()));
}
