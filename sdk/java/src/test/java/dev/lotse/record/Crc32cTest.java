package dev.lotse.record;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class Crc32cTest {
    @Test
    void vectorsFromTheBroker() {
        for (String[] v : Vectors.lines("crc32c.txt")) {
            byte[] input = Vectors.hex(v[1]);
            assertEquals(Long.parseLong(v[2], 16), Batch.crc(input, 0, input.length), v[0]);
        }
    }

    @Test
    void theStandardCheckValue() {
        byte[] input = "123456789".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        assertEquals(0xE3069283L, Batch.crc(input, 0, input.length));
    }
}
