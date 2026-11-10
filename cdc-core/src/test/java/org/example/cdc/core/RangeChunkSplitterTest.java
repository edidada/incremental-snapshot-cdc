package org.example.cdc.core;

import org.example.cdc.core.impl.RangeChunkSplitter;
import org.example.cdc.core.impl.SimulatedDatabase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RangeChunkSplitterTest {

    @Test
    void producesAnExactPartition() {
        TableRef ref = TableRef.numeric("t", "id");
        SimulatedDatabase db = new SimulatedDatabase(ref);
        for (long id = 1; id <= 23; id++) {
            db.seed(id, Record.row("id", id, "v", "x" + id));
        }

        List<Chunk> chunks = new RangeChunkSplitter(db).split(ref, 5);
        assertEquals(5, chunks.size()); // ceil(23 / 5)

        List<Record> union = new ArrayList<>();
        for (Chunk chunk : chunks) {
            union.addAll(db.select(chunk));
        }
        assertEquals(23, union.size(), "no duplicates across chunks");
        assertEquals(new HashSet<>(db.table().allRows()), new HashSet<>(union),
                "no rows lost");
    }

    @Test
    void emptyTableStillYieldsOneUnboundedChunk() {
        TableRef ref = TableRef.numeric("t", "id");
        SimulatedDatabase db = new SimulatedDatabase(ref);
        List<Chunk> chunks = new RangeChunkSplitter(db).split(ref, 10);
        assertEquals(1, chunks.size());
        Chunk c = chunks.get(0);
        assertTrue(c.contains(1L));
        assertTrue(c.contains(-999L));
    }

    @Test
    void chunkIdsAreDeterministic() {
        assertEquals("orders#00007", RangeChunkSplitter.chunkId("orders", 7));
        assertEquals("orders#00007", RangeChunkSplitter.chunkId("orders", 7));
    }
}
