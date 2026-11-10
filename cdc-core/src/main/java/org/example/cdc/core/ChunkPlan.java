package org.example.cdc.core;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The immutable chunk plan of a snapshot run plus its original start position.
 * Persisted at the beginning of a run so a resumed snapshot uses the exact same
 * chunk boundaries (recomputing them from a table that changed since the crash
 * would silently duplicate or lose rows).
 */
public final class ChunkPlan {

    private final String table;
    private final long snapshotStart;
    private final List<Chunk> chunks;

    public ChunkPlan(String table, long snapshotStart, List<Chunk> chunks) {
        this.table = Objects.requireNonNull(table, "table");
        this.snapshotStart = snapshotStart;
        this.chunks = Collections.unmodifiableList(
                new java.util.ArrayList<>(Objects.requireNonNull(chunks, "chunks")));
    }

    public String table() {
        return table;
    }

    public long snapshotStart() {
        return snapshotStart;
    }

    public List<Chunk> chunks() {
        return chunks;
    }

    @Override
    public String toString() {
        return "ChunkPlan{" + table + ", start=" + snapshotStart
                + ", chunks=" + chunks.size() + '}';
    }
}
