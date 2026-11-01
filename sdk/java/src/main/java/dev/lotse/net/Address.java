package dev.lotse.net;

import java.util.Objects;

/**
 * A broker address, {@code host:port}. {@code [::1]:9092} is an IPv6 address. The host is also the name TLS checks
 * the node's certificate against.
 *
 * @param host the host name or IP address, without brackets
 * @param port the port, 1 to 65535
 */
public record Address(String host, int port) {
    /** Checks the parts. */
    public Address {
        Objects.requireNonNull(host);
        if (host.isEmpty()) {
            throw new IllegalArgumentException("the host is empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port " + port + " is not from 1 to 65535");
        }
    }

    /** Parses {@code host:port} or {@code [ipv6]:port}. */
    public static Address parse(String text) {
        int colon = text.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("\"" + text + "\" is not host:port");
        }
        String host = text.substring(0, colon);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        } else if (host.contains(":")) {
            throw new IllegalArgumentException("\"" + text + "\": an IPv6 address needs brackets, [::1]:9092");
        }
        int port;
        try {
            port = Integer.parseInt(text.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("\"" + text + "\": the port is not a number");
        }
        return new Address(host, port);
    }

    @Override
    public String toString() {
        return host.contains(":") ? "[" + host + "]:" + port : host + ":" + port;
    }
}
