package org.example.cdc.core;

import org.example.cdc.core.engine.DefaultSnapshotWindow;
import org.example.cdc.core.impl.InMemoryChangeLog;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotWindowTest {

    @Test
    void openBufferReplayDeduplicateFlushFollowsDbLog() {
        TableRef ref = TableRef.numeric("t", "id");
        InMemoryChangeLog log = new InMemoryChangeLog();
        DefaultSnapshotWindow window = new DefaultSnapshotWindow(ref, log);

        window.open("t#00001");
        long low = log.currentPosition(); // 0
        assertTrue(window.isOpen());

        window.buffer(Record.of("t", 1L, "id", 1L, "v", "a"));
        window.buffer(Record.of("t", 2L, "id", 2L, "v", "b"));
        assertEquals(2, window.bufferedCount());

        // Concurrent binlog events committed inside (LOW, HIGH].
        log.append("t", ChangeType.UPDATE, 2L, Record.of("t", 2L, "id", 2L, "v", "b2"));
        log.append("t", ChangeType.DELETE, 3L, null);
        log.append("t", ChangeType.INSERT, 4L, Record.of("t", 4L, "id", 4L, "v", "d"));

        window.close("t#00001");
        long high = log.currentPosition(); // 3
        assertFalse(window.isOpen());
        assertEquals(low, window.low());
        assertEquals(high, window.high());

        List<ChangeEvent> events = window.replay();
        assertEquals(3, events.size());
        assertEquals(1L, events.get(0).position());
        assertEquals(3L, events.get(2).position());

        // Log wins: rows whose key appears in the window are removed.
        window.deduplicate(2L);
        assertEquals(1, window.bufferedCount());

        List<Record> flushed = window.flush();
        assertEquals(1, flushed.size());
        assertEquals(1L, flushed.get(0).primaryKey());
        assertEquals(0, window.bufferedCount());

        Watermark wm = window.watermark();
        assertFalse(wm.contains(0L));
        assertTrue(wm.contains(1L));
        assertTrue(wm.contains(3L));
        assertFalse(wm.contains(4L));
    }

    @Test
    void replayIsLimitedToTheWindow() {
        TableRef ref = TableRef.numeric("t", "id");
        InMemoryChangeLog log = new InMemoryChangeLog();
        log.append("t", ChangeType.INSERT, 1L, Record.of("t", 1L, "id", 1L)); // pos 1, before LOW

        DefaultSnapshotWindow window = new DefaultSnapshotWindow(ref, log);
        window.open("t#00002"); // LOW = 1
        log.append("t", ChangeType.UPDATE, 1L, Record.of("t", 1L, "id", 1L)); // pos 2, inside
        window.close("t#00002"); // HIGH = 2
        log.append("t", ChangeType.DELETE, 1L, null); // pos 3, after HIGH

        assertEquals(1, window.replay().size());
        assertEquals(2L, window.replay().get(0).position());
    }

    @Test
    void enforcesLifecycle() {
        TableRef ref = TableRef.numeric("t", "id");
        InMemoryChangeLog log = new InMemoryChangeLog();
        DefaultSnapshotWindow window = new DefaultSnapshotWindow(ref, log);

        assertThrows(IllegalStateException.class,
                () -> window.buffer(Record.of("t", 1L, "id", 1L)));
        assertThrows(IllegalStateException.class, () -> window.close("x"));
        assertThrows(IllegalStateException.class, window::flush);

        window.open("t#00003");
        assertThrows(IllegalStateException.class, () -> window.open("again"));
        window.close("t#00003");
        assertThrows(IllegalStateException.class, () -> window.close("again"));
        window.flush();
    }
}
