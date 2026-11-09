/** A broker address, `host:port`. `[::1]:9092` is an IPv6 address. The host is also the name TLS checks the node's certificate against. */
export interface Address {
  /** The host name or IP address, without brackets. */
  readonly host: string;
  /** 1 to 65535. */
  readonly port: number;
}

export function address(host: string, port: number): Address {
  if (host.length === 0) {
    throw new RangeError("the host is empty");
  }
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new RangeError(`port ${port} is not from 1 to 65535`);
  }
  return { host, port };
}

/** Parses `host:port` or `[ipv6]:port`. */
export function parseAddress(text: string): Address {
  const colon = text.lastIndexOf(":");
  if (colon < 0) {
    throw new RangeError(`"${text}" is not host:port`);
  }
  let host = text.slice(0, colon);
  if (host.startsWith("[") && host.endsWith("]")) {
    host = host.slice(1, -1);
  } else if (host.includes(":")) {
    throw new RangeError(`"${text}": an IPv6 address needs brackets, [::1]:9092`);
  }
  const digits = text.slice(colon + 1);
  if (!/^[0-9]+$/.test(digits)) {
    throw new RangeError(`"${text}": the port is not a number`);
  }
  return address(host, Number(digits));
}

export function formatAddress(a: Address): string {
  return a.host.includes(":") ? `[${a.host}]:${a.port}` : `${a.host}:${a.port}`;
}

/** A key for maps: two addresses with the same text are the same address. */
export function addressKey(a: Address): string {
  return formatAddress(a);
}
