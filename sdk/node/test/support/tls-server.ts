import tls from "node:tls";
import { readFileSync } from "node:fs";
import { decodeRequest, encodeResponse, withLengthPrefix } from "../../src/proto/index.js";
import type { Certs } from "./lotse.js";

/**
 * A TLS server in this process that answers `ApiVersions` with the feature "tls-test", with the node certificate that
 * `lotse certs` made. It can ask for a client certificate, and it records the subjects it saw.
 */
export class TlsTestServer {
  readonly clientSubjects: string[] = [];
  private readonly sockets = new Set<tls.TLSSocket>();

  private constructor(
    private readonly server: tls.Server,
    readonly port: number,
  ) {}

  static async start(
    certs: Pick<Certs, "ca" | "nodeCert" | "nodeKey">,
    options: { version?: tls.SecureVersion; requireClient?: boolean } = {},
  ): Promise<TlsTestServer> {
    const version = options.version ?? "TLSv1.3";
    let result: TlsTestServer | undefined;
    const server = tls.createServer(
      {
        cert: readFileSync(certs.nodeCert),
        key: readFileSync(certs.nodeKey),
        ca: readFileSync(certs.ca),
        requestCert: options.requireClient === true,
        rejectUnauthorized: options.requireClient === true,
        minVersion: version,
        maxVersion: version,
      },
      (socket) => {
        const self = result as TlsTestServer;
        self.sockets.add(socket);
        socket.on("close", () => self.sockets.delete(socket));
        socket.on("error", () => {});
        const peer = socket.getPeerCertificate();
        if (peer && peer.subject) {
          self.clientSubjects.push(`CN=${peer.subject.CN}`);
        }
        let buf = Buffer.alloc(0);
        socket.on("data", (chunk: Buffer) => {
          buf = Buffer.concat([buf, chunk]);
          while (buf.length >= 4) {
            const len = buf.readUInt32BE(0);
            if (buf.length < 4 + len) {
              return;
            }
            const frame = buf.subarray(4, 4 + len);
            buf = buf.subarray(4 + len);
            const req = decodeRequest(frame);
            socket.write(
              withLengthPrefix(
                encodeResponse(req.header.correlationId, { type: "ApiVersions", versions: [], features: ["tls-test"] }),
              ),
            );
          }
        });
      },
    );
    server.on("tlsClientError", () => {});
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    result = new TlsTestServer(server, (server.address() as import("node:net").AddressInfo).port);
    return result;
  }

  async close(): Promise<void> {
    for (const s of this.sockets) {
      s.destroy();
    }
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}
