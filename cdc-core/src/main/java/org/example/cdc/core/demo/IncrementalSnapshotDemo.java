package org.example.cdc.core.demo;

import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.SnapshotResult;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.impl.MaterializingSink;
import org.example.cdc.core.impl.SimulatedDatabase;
import org.example.cdc.core.spi.SnapshotListener;

import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Standalone, dependency-free demonstration of the incremental snapshot engine.
 *
 * <p>Seeds a 200-row table, then injects concurrent INSERT/UPDATE/DELETE while
 * the parallel chunk SELECTs are in flight. A DBLog window plus the unified
 * changelog replay must reconstruct a final state byte-for-byte equal to the
 * source table. A self-check throws on any mismatch, so this program doubles as
 * a smoke test (the GitHub Actions workflow executes it after the build).</p>
 */
public final class IncrementalSnapshotDemo {

    private IncrementalSnapshotDemo() {
    }

    public static void main(String[] args) {
        System.out.println("=== Incremental Snapshot CDC :: demo ===");

        TableRef ref = TableRef.numeric("orders", "id");
        SimulatedDatabase db = new SimulatedDatabase(ref);
        for (long id = 1; id <= 200; id++) {
            db.seed(id, Record.row("id", id, "amount", id * 10L, "status", "NEW"));
        }

        // Concurrent DML injected exactly once, inside a chunk's SELECT window.
        AtomicBoolean injected = new AtomicBoolean(false);
        db.setSelectHook(chunk -> {
            if (injected.compareAndSet(false, true)) {
                System.out.println("  [writer] concurrent DML fired during " + chunk);
                db.update(3L, Record.row("id", 3L, "amount", 9999L, "status", "UPDATED"));
                db.delete(7L);
                db.insert(201L, Record.row("id", 201L, "amount", 2010L, "status", "INSERTED"));
                db.update(42L, Record.row("id", 42L, "amount", 4242L, "status", "UPDATED"));
            }
        });

        MaterializingSink sink = new MaterializingSink(ref.order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(16)
                .setParallelism(4)
                .setMaxRetries(2)
                .setRetryBackoffMillis(10L);

        SnapshotResult result = db.coordinator(config, sink, SnapshotListener.noop()).execute();

        verify(db, sink, result);

        System.out.println("  " + result);
        System.out.println("  rows materialized : " + sink.distinctKeys());
        System.out.println("  changelog events  : " + sink.changelog().size());
        System.out.println("DEMO OK - final state is consistent under concurrent writes.");
    }

    private static void verify(SimulatedDatabase db, MaterializingSink sink, SnapshotResult result) {
        NavigableMap<Object, Record> expected = db.currentRows();
        NavigableMap<Object, Record> actual = sink.rows();
        if (!expected.equals(actual)) {
            throw new AssertionError("snapshot diverged from source: expected "
                    + expected.size() + " rows, got " + actual.size());
        }
        Record r3 = sink.get(3L);
        if (r3 == null || !"UPDATED".equals(r3.get("status"))) {
            throw new AssertionError("concurrent UPDATE was not applied");
        }
        if (sink.get(7L) != null) {
            throw new AssertionError("concurrent DELETE was not applied");
        }
        if (sink.get(201L) == null) {
            throw new AssertionError("concurrent INSERT was not applied");
        }
        Record r42 = sink.get(42L);
        if (r42 == null || !Long.valueOf(4242L).equals(r42.get("amount"))) {
            throw new AssertionError("concurrent UPDATE was not applied");
        }
        if (result.metrics().getParallelism() != 4
                || result.metrics().getChunksProcessed() <= 1) {
            throw new AssertionError("parallel chunk execution was not exercised");
        }
    }
}
