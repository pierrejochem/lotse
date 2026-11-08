import { AuthenticationError } from "../auth/errors.js";
import { type Address, addressKey } from "./address.js";
import { Connection } from "./connection.js";
import { ConnectionError } from "./errors.js";
import { type ConnectionOptions, resolveOptions } from "./options.js";

/**
 * One connection per broker address, authenticated, kept for reuse.
 *
 * `get` returns the live connection to an address, opening it on first use. A connection whose session is due for
 * renewal (four fifths of its lifetime) authenticates again on the same connection, as the same principal; one caller
 * renews while the others go on with the session, which still holds. A connection whose session has ended, or that
 * broke, is replaced by a new one with a fresh login. A connection that failed once should be handed to `discard`: it
 * is not trusted again. Callers that ask for the same address while it is being opened share the one attempt.
 */
export class ConnectionPool {
  private readonly connections = new Map<string, Connection>();
  private readonly opening = new Map<string, Promise<Connection>>();
  private readonly renewing = new Map<Connection, Promise<void>>();
  private isClosed = false;

  constructor(private readonly options: ConnectionOptions) {
    // Fail on a bad option now, not at the first get.
    resolveOptions(options);
  }

  /** The connection to `address`; see the class description for when it is opened, renewed or replaced. */
  async get(address: Address): Promise<Connection> {
    if (this.isClosed) {
      throw new ConnectionError("the pool is closed", false);
    }
    const key = addressKey(address);
    const cached = this.connections.get(key);
    if (cached !== undefined) {
      if (!cached.isOpen) {
        this.discard(cached);
      } else {
        switch (cached.sessionState()) {
          case "UNLIMITED":
          case "ACTIVE":
            return cached;
          case "ENDED":
            this.discard(cached); // requests would be refused: start over
            break;
          case "RENEW_DUE": {
            const credentials = this.options.credentials;
            if (credentials === undefined) {
              return cached;
            }
            let renewal = this.renewing.get(cached);
            if (renewal === undefined) {
              renewal = cached.authenticate(credentials).finally(() => this.renewing.delete(cached));
              this.renewing.set(cached, renewal);
            }
            try {
              await renewal;
              return cached;
            } catch (e) {
              this.discard(cached);
              if (e instanceof AuthenticationError) {
                throw e;
              }
              // a failure of the connection itself: log in on a new one below
            }
            break;
          }
        }
      }
    }
    let pending = this.opening.get(key);
    if (pending === undefined) {
      pending = Connection.open(address, this.options).then(
        (c) => {
          this.opening.delete(key);
          if (this.isClosed) {
            c.close();
            throw new ConnectionError("the pool is closed", false);
          }
          this.connections.set(key, c);
          return c;
        },
        (e: unknown) => {
          this.opening.delete(key);
          throw e;
        },
      );
      this.opening.set(key, pending);
    }
    return pending;
  }

  /** Forgets and closes `connection`, if the pool still holds it. */
  discard(connection: Connection): void {
    const key = addressKey(connection.address);
    if (this.connections.get(key) === connection) {
      this.connections.delete(key);
    }
    this.renewing.delete(connection);
    connection.close();
  }

  /** Closes every connection. */
  close(): void {
    this.isClosed = true;
    for (const c of this.connections.values()) {
      c.close();
    }
    this.connections.clear();
    this.renewing.clear();
  }
}
