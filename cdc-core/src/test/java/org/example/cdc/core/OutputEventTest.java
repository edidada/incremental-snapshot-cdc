package org.example.cdc.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutputEventTest {

    @Test
    void baselineLosesToStreamAtTheSamePosition() {
        Record row = Record.of("t", 1L, "id", 1L);
        OutputEvent baseline = OutputEvent.baseline(5L, row);
        OutputEvent stream = OutputEvent.fromChangeEvent(
                ChangeEvent.update(5L, "t", 1L, row));

        assertEquals(OutputEvent.BASELINE_PRIORITY, baseline.priority());
        assertEquals(OutputEvent.STREAM_PRIORITY, stream.priority());
        assertTrue(OutputEvent.ORDER.compare(baseline, stream) < 0,
                "baseline must be applied before a stream event at the same position");
    }

    @Test
    void deleteEventIsMarkedAsDelete() {
        OutputEvent delete = OutputEvent.fromChangeEvent(ChangeEvent.delete(9L, "t", 1L));
        assertTrue(delete.isDelete());
        assertEquals(OutputEvent.Kind.DELETE, delete.kind());
        assertFalse(OutputEvent.baseline(1L, Record.of("t", 1L, "id", 1L)).isDelete());
    }
}
