package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.lotse.proto.Messages.Config;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueueOptionsTest {
    @Test
    void aLengthLimitTravelsAsItsSettings() {
        // ADR 0118.
        QueueOptions o = QueueOptions.builder().maxLength(1000).maxLengthBytes(65536).overflow("drop-head").build();
        assertEquals(
                List.of(
                        new Config("max.length", "1000"),
                        new Config("max.length.bytes", "65536"),
                        new Config("overflow", "drop-head")),
                o.configs());
        assertEquals(List.of(), QueueOptions.defaults().configs());
    }

    @Test
    void aLengthLimitOutOfRangeIsRefusedBeforeAnythingIsSent() {
        assertThrows(IllegalArgumentException.class, () -> QueueOptions.builder().maxLength(0));
        assertThrows(IllegalArgumentException.class, () -> QueueOptions.builder().maxLengthBytes(0));
        assertThrows(IllegalArgumentException.class, () -> QueueOptions.builder().overflow("reject-publish-dlx"));
    }
}
