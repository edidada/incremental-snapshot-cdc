package org.example.cdc.it;

import org.example.cdc.core.OutputEvent;
import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.SnapshotResult;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.impl.MaterializingSink;
import org.example.cdc.core.impl.SimulatedDatabase;
import org.example.cdc.core.spi.SnapshotListener;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Stress: large table, parallel chunks, heavy concurrent DML, exactly-once output. */
class ParallelSnapshotConsistencyTest {

    @Test
    void largeTableWithHeavyConcurrentWritesReconcilesExactlyOnce() {
        TableRef ref = TableRef.numeric("orders", "id");
        SimulatedDatabase db = new SimulatedDatabase(ref);
        int rows = 1000;
        for (long id = 1; id <= rows; id++) {
            db.seed(id, Record.row("id", id, "amount", id * 10L, "status", "NEW"));
        }

        // All writes are injected inside one chunk's SELECT window by a pool of
        // writer threads, so the test is fully deterministic (no sleeps/races on
        // whether writes land before snapshotEnd).
        AtomicBoolean once = new AtomicBoolean(false);
        db.setSelectHook(chunk -> {
            if (once.compareAndSet(false, true)) {
                List<Thread> writers = new ArrayList<>();
                for (int w = 0; w < 4; w++) {
                    final int base = w;
                    Thread t = new Thread(() -> {
                        for (int k = 0; k < 50; k++) {
                            long id = 1L + base * 50L + k;
                            db.update(id, Record.row("id", id, "amount", id * 7L,
                                    "status", "W" + base));
                        }
                    });
                    writers.add(t);
                    t.start();
                }
                for (Thread t : writers) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                for (long id = 901; id <= 920; id++) {
                    db.delete(id);
                }
                for (long id = 1001; id <= 1020; id++) {
                    db.insert(id, Record.row("id", id, "amount", id, "status", "NEW"));
                }
            }
        });

        MaterializingSink sink = new MaterializingSink(ref.order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(32)
                .setParallelism(8)
                .setResumeFromCheckpoint(false);
        SnapshotResult result = db.coordinator(config, sink, SnapshotListener.noop()).execute();

        assertEquals(db.currentRows(), sink.rows());
        assertEquals(1000, sink.rows().size()); // 1000 - 20 deletes + 20 inserts
        assertEquals(32, result.metrics().getChunksProcessed()); // ceil(1000/32)
        assertEquals(8, result.metrics().getParallelism());
        assertEquals(240, result.metrics().getStreamEvents()); // 200 updates + 20 deletes + 20 inserts

        // A key that was never touched must appear exactly once (baseline only).
        long untouchedEvents = sink.changelog().stream()
                .filter(e -> e.primaryKey().equals(500L))
                .count();
        assertEquals(1L, untouchedEvents);
        assertTrue(sink.changelog().stream().anyMatch(OutputEvent::isDelete));
    }
}
