import net from "node:net";
import {
  decodeRequest,
  type DecodedRequest,
  type DeliverPush,
  encodePush,
  encodeResponse,
  type Response,
  withLengthPrefix,
} from "../../src/proto/index.js";
import { type Address, address } from "../../src/net/address.js";

/** One accepted connection. */
export class Peer {
  readonly received: DecodedRequest[] = [];

  constructor(private readonly socket: net.Socket) {}

  reply(correlationId: number, response: Response): void {
    this.raw(encodeResponse(correlationId, response));
  }

  push(push: DeliverPush): void {
    this.raw(encodePush(push));
  }

  raw(frame: Uint8Array): void {
    this.socket.write(withLengthPrefix(frame));
  }

  rawBytes(bytes: Uint8Array): void {
    this.socket.write(bytes);
  }

  closeSocket(): void {
    this.socket.destroy();
  }
}

/** What a test does with each request. */
export type Handler = (peer: Peer, request: DecodedRequest) => void | Promise<void>;

/** A broker in this process, on loopback, that answers as a test tells it to. Plain TCP. */
export class FakeBroker {
  accepted = 0;
  readonly peers: Peer[] = [];
  private readonly sockets = new Set<net.Socket>();

  private constructor(
    private readonly server: net.Server,
    readonly address: Address,
  ) {}

  static async start(handler: Handler): Promise<FakeBroker> {
    let broker: FakeBroker | undefined;
    const server = net.createServer((socket) => {
      const b = broker as FakeBroker;
      b.accepted++;
      b.sockets.add(socket);
      socket.on("error", () => {});
      socket.on("close", () => b.sockets.delete(socket));
      const peer = new Peer(socket);
      b.peers.push(peer);
      let buf = Buffer.alloc(0);
      socket.on("data", (chunk) => {
        buf = Buffer.concat([buf, chunk]);
        while (buf.length >= 4) {
          const len = buf.readUInt32BE(0);
          if (buf.length < 4 + len) {
            return;
          }
          const frame = buf.subarray(4, 4 + len);
          buf = buf.subarray(4 + len);
          const request = decodeRequest(frame);
          peer.received.push(request);
          // A handler that throws closes the peer's connection: tests use it to break one on purpose.
          try {
            void Promise.resolve(handler(peer, request)).catch(() => peer.closeSocket());
          } catch {
            peer.closeSocket();
          }
        }
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as net.AddressInfo).port;
    broker = new FakeBroker(server, address("127.0.0.1", port));
    return broker;
  }

  async close(): Promise<void> {
    for (const s of this.sockets) {
      s.destroy();
    }
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

/** Runs `body` with a started broker and closes it afterwards. */
export async function withBroker<T>(handler: Handler, body: (broker: FakeBroker) => Promise<T>): Promise<T> {
  const broker = await FakeBroker.start(handler);
  try {
    return await body(broker);
  } finally {
    await broker.close();
  }
}
