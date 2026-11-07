package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.lotse.proto.Messages.BindingMetadata;
import dev.lotse.proto.Messages.HeaderMatch;
import dev.lotse.record.ByteString;
import dev.lotse.record.Header;
import dev.lotse.record.Vectors;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/** Exchange routing against {@code routing.txt}, which {@code lotse-route} writes. */
class RoutingConformanceTest {
    /** {@code -} for null, {@code =hex} for bytes. */
    private static ByteString nullable(String s) {
        return s.equals("-") ? null : ByteString.copyOf(Vectors.hex(s.substring(1)));
    }

    private static String text(String s) {
        return new String(nullable(s).toByteArray(), StandardCharsets.UTF_8);
    }

    private static List<Header> headers(String s) {
        List<Header> out = new ArrayList<>();
        if (!s.equals("-")) {
            for (String h : s.split("\\+")) {
                String[] kv = h.split(":", 2);
                out.add(new Header(ByteString.copyOf(Vectors.hex(kv[0])), nullable(kv[1])));
            }
        }
        return out;
    }

    @Test
    void routesAsTheBrokerDoes() {
        int n = 0;
        for (String[] v : Vectors.lines("routing.txt")) {
            List<BindingMetadata> bindings = new ArrayList<>();
            if (!v[2].equals("-")) {
                for (String b : v[2].split(";")) {
                    String[] f = b.split(",", 4);
                    List<HeaderMatch> matches = new ArrayList<>();
                    for (Header h : headers(f[3])) {
                        matches.add(new HeaderMatch(h.key(), h.value()));
                    }
                    bindings.add(new BindingMetadata(Long.parseLong(f[0]), text(f[1]), matches, f[2].equals("all")));
                }
            }
            Exchange x = new Exchange("x", 9, Integer.parseInt(v[1]), bindings);
            TreeSet<Long> want = new TreeSet<>();
            if (!v[5].equals("-")) {
                for (String q : v[5].split(",")) {
                    want.add(Long.parseLong(q));
                }
            }
            assertEquals(want, x.route(text(v[3]), headers(v[4])), v[0]);
            n++;
        }
        assertEquals(41, n, "every vector read");
    }

    @Test
    void anUnknownTypeRoutesNowhere() {
        Exchange x = new Exchange("x", 9, 7, List.of(new BindingMetadata(1, "k", List.of(), true)));
        assertEquals(new TreeSet<Long>(), x.route("k", List.of()));
    }
}
