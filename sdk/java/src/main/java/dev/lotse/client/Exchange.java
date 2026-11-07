package dev.lotse.client;

import dev.lotse.proto.Messages.BindingMetadata;
import dev.lotse.proto.Messages.ExchangeMetadata;
import dev.lotse.proto.Messages.HeaderMatch;
import dev.lotse.record.Header;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * An exchange and its bindings, as metadata gives them, and which queues a message goes to. The rules are those of
 * {@code lotse-route}, which the broker and the Rust client use, checked against {@code routing.txt}:
 *
 * <ul>
 *   <li><b>direct</b> ({@code 0}): the binding key equals the routing key;
 *   <li><b>topic</b> ({@code 1}): keys are words split at {@code .}; in a binding key {@code *} matches exactly one
 *       word and {@code #} zero or more; an empty key has no words;
 *   <li><b>fanout</b> ({@code 2}): every binding;
 *   <li><b>headers</b> ({@code 3}): the binding's headers must be present with equal values (a null value equals
 *       only null), all of them or any one; a binding without headers matches everything when it asks for all and
 *       nothing when it asks for any.
 * </ul>
 *
 * <p>A queue bound twice gets a message once. An exchange of a type this library does not know routes nowhere.
 */
record Exchange(String name, long resourceId, int type, List<BindingMetadata> bindings) {
    static final int DIRECT = 0;
    static final int TOPIC = 1;
    static final int FANOUT = 2;
    static final int HEADERS = 3;

    Exchange {
        Objects.requireNonNull(name);
        bindings = List.copyOf(bindings);
    }

    static Exchange of(String name, long resourceId, ExchangeMetadata x) {
        return new Exchange(name, resourceId, x.exchangeType(), x.bindings());
    }

    /** The ids of the queues a message with {@code routingKey} and {@code headers} goes to, ascending. */
    TreeSet<Long> route(String routingKey, List<Header> headers) {
        TreeSet<Long> out = new TreeSet<>(Long::compareUnsigned);
        for (BindingMetadata b : bindings) {
            boolean hit = switch (type) {
                case DIRECT -> b.bindingKey().equals(routingKey);
                case TOPIC -> topicMatches(b.bindingKey(), routingKey);
                case FANOUT -> true;
                case HEADERS -> headersMatch(b, headers);
                default -> false;
            };
            if (hit) {
                out.add(b.queueId());
            }
        }
        return out;
    }

    /** Whether a topic binding key matches a routing key. */
    static boolean topicMatches(String pattern, String key) {
        return matches(words(pattern), 0, words(key), 0);
    }

    /** An empty key has no words, not one empty word. */
    private static List<String> words(String s) {
        return s.isEmpty() ? List.of() : Arrays.asList(s.split("\\.", -1));
    }

    private static boolean matches(List<String> pattern, int p, List<String> key, int k) {
        if (p == pattern.size()) {
            return k == key.size();
        }
        String w = pattern.get(p);
        if (w.equals("#")) {
            // # takes zero words, or one and stays.
            return matches(pattern, p + 1, key, k) || (k < key.size() && matches(pattern, p, key, k + 1));
        }
        if (k == key.size()) {
            return false;
        }
        return (w.equals("*") || w.equals(key.get(k))) && matches(pattern, p + 1, key, k + 1);
    }

    private static boolean headersMatch(BindingMetadata b, List<Header> present) {
        if (b.headerMatch().isEmpty()) {
            return b.matchAll();
        }
        List<Boolean> hits = new ArrayList<>();
        for (HeaderMatch m : b.headerMatch()) {
            hits.add(present.stream()
                    .anyMatch(h -> h.key().equals(m.key()) && Objects.equals(h.value(), m.value())));
        }
        return b.matchAll() ? !hits.contains(false) : hits.contains(true);
    }
}
