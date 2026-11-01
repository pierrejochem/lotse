package dev.lotse.client;

import dev.lotse.proto.ErrorCode;
import java.time.Duration;
import java.util.List;

/**
 * What went wrong in a client call. The subclasses mirror the Rust client's {@code ClientError}; each says what
 * the caller can do next.
 *
 * <p>The exceptions are unchecked. Switch on the type with a pattern, or catch the one you can act on:
 * {@link Unknown} is the one that needs a decision, because a write may or may not have happened.
 */
public abstract sealed class ClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    ClientException(String message) {
        super(message);
    }

    /** The broker refused the request with an error code. */
    public static final class Broker extends ClientException {
        private static final long serialVersionUID = 1L;

        private final transient ErrorCode code;
        private final String brokerMessage;

        /** Creates the exception; {@code message} is the broker's explanation or {@code null}. */
        public Broker(ErrorCode code, String message) {
            super("broker error " + code + (message == null ? "" : ": " + message));
            this.code = code;
            this.brokerMessage = message;
        }

        /** The error code. */
        public ErrorCode code() {
            return code;
        }

        /** The broker's explanation, or {@code null}. */
        public String brokerMessage() {
            return brokerMessage;
        }
    }

    /** The offset is not in the partition: retention deleted it (it is below the log start), or it is past the end. */
    public static final class OffsetOutOfRange extends ClientException {
        private static final long serialVersionUID = 1L;

        private final String stream;
        private final int partition;
        private final long offset;
        private final long logStart;

        /** Creates the exception. */
        public OffsetOutOfRange(String stream, int partition, long offset, long logStart) {
            super("offset " + Long.toUnsignedString(offset) + " is not in " + stream + " partition " + partition
                    + ", which starts at " + Long.toUnsignedString(logStart));
            this.stream = stream;
            this.partition = partition;
            this.offset = offset;
            this.logStart = logStart;
        }

        /** The stream. */
        public String stream() {
            return stream;
        }

        /** The partition. */
        public int partition() {
            return partition;
        }

        /** The offset asked for, a {@code u64}. */
        public long offset() {
            return offset;
        }

        /** The first offset the partition still holds, a {@code u64}. Reading on from there skips what retention deleted. */
        public long logStart() {
            return logStart;
        }
    }

    /**
     * A produce may or may not have been written. Read the partition to find out, or produce again and accept a
     * possible duplicate. The client has started over with a new producer id, so later produces do not wait for a
     * gap that may never fill.
     */
    public static final class Unknown extends ClientException {
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public Unknown(String why) {
            super("outcome unknown: " + why);
        }
    }

    /** The transaction can no longer be committed (a write's outcome is unknown, or the producer was fenced): abort it. */
    public static final class MustAbort extends ClientException {
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public MustAbort(String why) {
            super("the transaction must be aborted: " + why);
        }
    }

    /** No transactional id is configured. */
    public static final class NotTransactional extends ClientException {
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public NotTransactional() {
            super("no transactional id is configured");
        }
    }

    /** No broker could serve the request before the retry time ran out. Nothing was written. */
    public static final class GaveUp extends ClientException {
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public GaveUp(Duration after, String why) {
            super("gave up after " + after + ": " + why);
        }
    }

    /** The stream, queue or exchange does not exist, or has no such partition. */
    public static final class UnknownStream extends ClientException {
        private static final long serialVersionUID = 1L;

        private final String name;
        private final int partition;

        /** Creates the exception. */
        public UnknownStream(String name, int partition) {
            super("unknown stream " + name + " or partition " + partition);
            this.name = name;
            this.partition = partition;
        }

        /** The name asked for. */
        public String name() {
            return name;
        }

        /** The partition asked for. */
        public int partition() {
            return partition;
        }
    }

    /** A mandatory message matched no binding. */
    public static final class Unroutable extends ClientException {
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public Unroutable(String exchange, String routingKey) {
            super("no queue is bound to " + exchange + " for routing key \"" + routingKey + "\"");
        }
    }

    /** A node asked directly did not answer. */
    public static final class NoAnswer extends ClientException {
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public NoAnswer(long node, String why) {
            super("node " + node + " did not answer: " + why);
        }
    }

    /** A resource with that name exists already (perhaps created by an earlier attempt of this same request). */
    public static final class ResourceExists extends ClientException {
        private static final long serialVersionUID = 1L;

        private final String name;
        private final long resourceId;

        /** Creates the exception. */
        public ResourceExists(String name, long resourceId) {
            super(name + " exists already (id " + Long.toUnsignedString(resourceId) + ")");
            this.name = name;
            this.resourceId = resourceId;
        }

        /** The name. */
        public String name() {
            return name;
        }

        /** The existing resource's id, a {@code u64}. */
        public long resourceId() {
            return resourceId;
        }
    }

    /** The node did not accept the credentials, or does not offer the mechanism. Retrying with the same credentials will not help. */
    public static final class Authentication extends ClientException {
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public Authentication(String why) {
            super("authentication failed: " + why);
        }
    }

    /**
     * A published message was written to some of its queues only. Each failure says whether that write is unknown
     * ({@link Unknown}) or did not happen.
     */
    public static final class NotAllWritten extends ClientException {
        private static final long serialVersionUID = 1L;

        private final transient List<String> written;
        private final transient List<ClientException> failures;

        /** Creates the exception: {@code written} names the queues that hold the message, {@code failures} says why the others do not. */
        public NotAllWritten(List<String> written, List<ClientException> failures) {
            super("published to some queues only: written to " + written + ", failed " + failures);
            this.written = List.copyOf(written);
            this.failures = List.copyOf(failures);
        }

        /** The queues that hold the message. */
        public List<String> written() {
            return written;
        }

        /** Why the other queues do not. */
        public List<ClientException> failures() {
            return failures;
        }
    }
}
