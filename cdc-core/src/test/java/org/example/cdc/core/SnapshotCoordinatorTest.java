package org.example.cdc.core;

import org.example.cdc.core.engine.SnapshotCoordinator;
import org.example.cdc.core.impl.InMemoryProgressStore;
import org.example.cdc.core.impl.MaterializingSink;
import org.example.cdc.core.impl.RangeChunkSplitter;
import org.example.cdc.core.impl.SimulatedDatabase;
import org.example.cdc.core.spi.SnapshotListener;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SnapshotCoordinatorTest {

    private static SimulatedDatabase seeded(int rows) {
        TableRef ref = TableRef.numeric("orders", "id");
        SimulatedDatabase db = new SimulatedDatabase(ref);
        for (long id = 1; id <= rows; id++) {
            db.seed(id, Record.row("id", id, "amount", id * 10L, "status", "NEW"));
        }
        return db;
    }

    @Test
    void fullSnapshotReproducesTheSourceExactly() {
        SimulatedDatabase db = seeded(50);
        MaterializingSink sink = new MaterializingSink(db.ref().order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(7)
                .setParallelism(4)
                .setResumeFromCheckpoint(false);

        SnapshotResult result = db.coordinator(config, sink, SnapshotListener.noop()).execute();

        assertEquals(db.currentRows(), sink.rows());
        assertEquals(50, sink.distinctKeys());
        assertEquals(8, result.metrics().getChunksProcessed()); // ceil(50/7)
        assertEquals(0, result.metrics().getStreamEvents());
        assertEquals(4, result.metrics().getParallelism());
    }

    @Test
    void concurrentDmlDuringWindowsIsFullyReconciled() {
        SimulatedDatabase db = seeded(50);
        AtomicBoolean once = new AtomicBoolean(false);
        db.setSelectHook(chunk -> {
            if (once.compareAndSet(false, true)) {
                db.update(3L, Record.row("id", 3L, "amount", 777L, "status", "UPDATED"));
                db.delete(7L);
                db.insert(51L, Record.row("id", 51L, "amount", 510L, "status", "INSERTED"));
            }
        });

        MaterializingSink sink = new MaterializingSink(db.ref().order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(8)
                .setParallelism(4)
                .setResumeFromCheckpoint(false);
        db.coordinator(config, sink, SnapshotListener.noop()).execute();

        assertEquals(db.currentRows(), sink.rows());
        assertEquals(50, sink.distinctKeys()); // 50 - 1 delete + 1 insert
        assertEquals("UPDATED", sink.get(3L).get("status"));
        assertNull(sink.get(7L));
        assertEquals("INSERTED", sink.get(51L).get("status"));
    }

    @Test
    void emptyTableStillCapturesConcurrentInsertsViaTheStream() {
        SimulatedDatabase db = seeded(0);
        AtomicBoolean once = new AtomicBoolean(false);
        db.setSelectHook(chunk -> {
            if (once.compareAndSet(false, true)) {
                db.insert(1L, Record.row("id", 1L, "v", "a"));
                db.insert(2L, Record.row("id", 2L, "v", "b"));
                db.insert(3L, Record.row("id", 3L, "v", "c"));
            }
        });

        MaterializingSink sink = new MaterializingSink(db.ref().order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(10)
                .setParallelism(2)
                .setResumeFromCheckpoint(false);
        SnapshotResult result = db.coordinator(config, sink, SnapshotListener.noop()).execute();

        assertEquals(db.currentRows(), sink.rows());
        assertEquals(3, sink.distinctKeys());
        assertEquals(3, result.metrics().getStreamEvents());
    }

    @Test
    void disabledResumeIgnoresAnExistingCheckpoint() {
        SimulatedDatabase db = seeded(20);
        InMemoryProgressStore store = new InMemoryProgressStore();
        store.markCompleted("orders", RangeChunkSplitter.chunkId("orders", 0), 0L);

        MaterializingSink sink = new MaterializingSink(db.ref().order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(5)
                .setParallelism(1)
                .setResumeFromCheckpoint(false);
        SnapshotCoordinator coordinator = db.coordinator(config, sink,
                SnapshotListener.noop(), store, new RangeChunkSplitter(db));
        SnapshotResult result = coordinator.execute();

        assertEquals(0, result.metrics().getChunksSkipped());
        assertEquals(db.currentRows(), sink.rows());
    }
}
