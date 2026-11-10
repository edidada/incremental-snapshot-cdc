package org.example.cdc.it;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.PrimaryKeyComparator;
import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.impl.MaterializingSink;
import org.example.cdc.core.impl.RangeChunkSplitter;
import org.example.cdc.core.impl.SimulatedDatabase;
import org.example.cdc.core.spi.SnapshotListener;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces the Flink-CDC string-collation trap: database {@code ORDER BY} uses
 * {@code utf8mb4_general_ci} (case-insensitive) while a naive Java splitter uses
 * binary {@code String.compareTo}. When the two orders disagree, chunk ranges
 * are no longer a partition and rows are duplicated or lost.
 */
class CollationBoundaryTest {

    private static final String[] CODES = {
            "Apple", "banana", "Cherry", "date", "Fig", "grape", "Kiwi", "lemon",
            "Mango", "nut", "Olive", "peach", "Quince", "raspberry", "Strawberry", "tangerine"
    };

    private static SimulatedDatabase database() {
        TableRef ref = TableRef.caseInsensitiveString("customers", "code");
        SimulatedDatabase db = new SimulatedDatabase(ref);
        for (String code : CODES) {
            db.seed(code, Record.row("code", code, "name", "n-" + code));
        }
        return db;
    }

    @Test
    void matchingCollationProducesAnExactPartition() {
        SimulatedDatabase db = database();
        List<Chunk> chunks = new RangeChunkSplitter(db).split(db.ref(), 4);

        List<Record> union = new ArrayList<>();
        for (Chunk chunk : chunks) {
            union.addAll(db.select(chunk));
        }
        assertEquals(CODES.length, union.size(), "no duplicates");
        assertEquals(new HashSet<>(db.table().allRows()), new HashSet<>(union), "no loss");
    }

    @Test
    void binarySplitOrderAgainstCiCollationCorruptsTheSnapshot() {
        SimulatedDatabase db = database();
        // Boundaries chosen with binary String.compareTo, but the DB evaluates
        // WHERE with its case-insensitive collation -> ranges are not a partition.
        List<Chunk> chunks = new RangeChunkSplitter(db,
                PrimaryKeyComparator.binaryString()).split(db.ref(), 4);

        List<Object> union = new ArrayList<>();
        for (Chunk chunk : chunks) {
            for (Record row : db.select(chunk)) {
                union.add(row.primaryKey());
            }
        }
        boolean corrupted = union.size() != CODES.length
                || new HashSet<>(union).size() != CODES.length;
        assertTrue(corrupted,
                "mismatched comparator must produce duplicate and/or missing rows");
    }

    @Test
    void fullPipelineWithCiKeysAndConcurrentWritesIsConsistent() {
        SimulatedDatabase db = database();
        AtomicBoolean once = new AtomicBoolean(false);
        db.setSelectHook(chunk -> {
            if (once.compareAndSet(false, true)) {
                db.update("Mango", Record.row("code", "Mango", "name", "UPDATED"));
                db.delete("date");
                db.insert("Zucchini", Record.row("code", "Zucchini", "name", "NEW"));
            }
        });

        MaterializingSink sink = new MaterializingSink(db.ref().order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(3)
                .setParallelism(4)
                .setResumeFromCheckpoint(false);
        db.coordinator(config, sink, SnapshotListener.noop()).execute();

        assertEquals(db.currentRows(), sink.rows());
        assertEquals("UPDATED", sink.get("Mango").get("name"));
        assertNull(sink.get("date"));
        assertNotNull(sink.get("Zucchini"));
    }
}
