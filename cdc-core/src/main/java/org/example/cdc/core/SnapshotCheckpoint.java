package org.example.cdc.core;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Durable progress of one in-flight snapshot, used to resume after a crash. */
public final class SnapshotCheckpoint {

    private final String table;
    private final long snapshotStart;
    private final Set<String> completedChunkIds;
    private final long updatedAtMillis;

    public SnapshotCheckpoint(String table, long snapshotStart,
                              Set<String> completedChunkIds, long updatedAtMillis) {
        this.table = Objects.requireNonNull(table, "table");
        this.snapshotStart = snapshotStart;
        this.completedChunkIds = Collections.unmodifiableSet(
                new LinkedHashSet<>(Objects.requireNonNull(completedChunkIds, "completedChunkIds")));
        this.updatedAtMillis = updatedAtMillis;
    }

    public SnapshotCheckpoint withCompleted(String chunkId) {
        Set<String> next = new LinkedHashSet<>(completedChunkIds);
        next.add(chunkId);
        return new SnapshotCheckpoint(table, snapshotStart, next, System.currentTimeMillis());
    }

    public String table() {
        return table;
    }

    public long snapshotStart() {
        return snapshotStart;
    }

    public Set<String> completedChunkIds() {
        return completedChunkIds;
    }

    public long updatedAtMillis() {
        return updatedAtMillis;
    }

    @Override
    public String toString() {
        return "SnapshotCheckpoint{" + table + ", start=" + snapshotStart
                + ", completed=" + completedChunkIds.size() + '}';
    }
}
