package org.example.cdc.it;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotCheckpoint;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.SnapshotResult;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.engine.SnapshotCoordinator;
import org.example.cdc.core.impl.InMemoryProgressStore;
import org.example.cdc.core.impl.MaterializingSink;
import org.example.cdc.core.impl.RangeChunkSplitter;
import org.example.cdc.core.impl.SimulatedDatabase;
import org.example.cdc.core.spi.SnapshotListener;
import org.example.cdc.core.spi.SnapshotReader;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Crash mid-snapshot, then resume: completed chunks are skipped, state is complete. */
class CheckpointRecoveryTest {

    @Test
    void resumeSkipsCompletedChunksAndStillProducesFullState() {
        TableRef ref = TableRef.numeric("orders", "id");
        SimulatedDatabase db = new SimulatedDatabase(ref);
        for (long id = 1; id <= 40; id++) {
            db.seed(id, Record.row("id", id, "amount", id * 10L, "status", "NEW"));
        }
        AtomicBoolean once = new AtomicBoolean(false);
        db.setSelectHook(chunk -> {
            if (once.compareAndSet(false, true)) {
                db.update(3L, Record.row("id", 3L, "amount", 999L, "status", "UPDATED"));
                db.delete(7L);
                db.insert(41L, Record.row("id", 41L, "amount", 410L, "status", "INSERTED"));
            }
        });

        InMemoryProgressStore store = new InMemoryProgressStore();
        CountingReader reader = new CountingReader(db);
        MaterializingSink sink = new MaterializingSink(ref.order()); // shared across both runs
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(5)
                .setParallelism(1) // deterministic crash point
                .setResumeFromCheckpoint(true);

        // Run 1: fail while starting the 3rd chunk, leaving the first two checkpointed.
        SnapshotCoordinator first = new SnapshotCoordinator(ref, new RangeChunkSplitter(db),
                reader, db.changeLog(), store, sink, config, crashingListener());
        assertThrows(RuntimeException.class, first::execute);

        SnapshotCheckpoint checkpoint = store.loadCheckpoint("orders").orElseThrow();
        Set<String> completed = checkpoint.completedChunkIds();
        assertEquals(2, completed.size());
        assertEquals(0L, checkpoint.snapshotStart());
        for (String id : completed) {
            assertEquals(1, reader.count(id), "crashed run selected chunk " + id);
        }

        // Run 2: resume with the same sink, progress store, changelog and reader.
        SnapshotCoordinator second = new SnapshotCoordinator(ref, new RangeChunkSplitter(db),
                reader, db.changeLog(), store, sink, config, SnapshotListener.noop());
        SnapshotResult result = second.execute();

        assertEquals(2, result.metrics().getChunksSkipped());
        assertEquals(6, result.metrics().getChunksProcessed());
        assertEquals(0L, result.snapshotStart(), "resume must reuse the original start position");
        assertEquals(db.currentRows(), sink.rows());
        for (String id : completed) {
            assertEquals(1, reader.count(id), "completed chunk was re-read on resume");
        }
        assertTrue(store.loadCheckpoint("orders").isEmpty(), "checkpoint cleared after success");
    }

    private static SnapshotListener crashingListener() {
        return new SnapshotListener() {
            @Override
            public void onChunkStarted(Chunk chunk) {
                if (chunk.index() >= 2) {
                    throw new IllegalStateException("simulated crash at " + chunk.id());
                }
            }
        };
    }

    private static final class CountingReader implements SnapshotReader {
        private final SimulatedDatabase db;
        private final Map<String, Integer> counts = new ConcurrentHashMap<>();

        CountingReader(SimulatedDatabase db) {
            this.db = db;
        }

        @Override
        public List<Record> select(Chunk chunk) {
            counts.merge(chunk.id(), 1, Integer::sum);
            return db.select(chunk);
        }

        int count(String chunkId) {
            return counts.getOrDefault(chunkId, 0);
        }
    }
}
