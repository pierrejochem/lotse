package dev.lotse.record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Reads the golden vectors that the Rust side writes to {@code sdk/conformance}. */
public final class Vectors {
    private Vectors() {}

    /** The directory of the vectors. */
    public static Path dir() {
        String p = System.getProperty("lotse.conformance");
        if (p == null) {
            throw new IllegalStateException("system property lotse.conformance is not set");
        }
        return Path.of(p);
    }

    /** The lines of a vector file, split at spaces, comments and blank lines left out. */
    public static List<String[]> lines(String file) {
        try {
            List<String[]> out = new ArrayList<>();
            for (String line : Files.readAllLines(dir().resolve(file), StandardCharsets.UTF_8)) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    out.add(line.split(" "));
                }
            }
            if (out.isEmpty()) {
                throw new IllegalStateException(file + " holds no vectors");
            }
            return out;
        } catch (IOException e) {
            throw new IllegalStateException("reading " + file, e);
        }
    }

    public static byte[] hex(String s) {
        if (s.equals("-")) {
            return new byte[0];
        }
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    public static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }
}
