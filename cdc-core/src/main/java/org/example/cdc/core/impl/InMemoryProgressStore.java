package org.example.cdc.core.impl;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.ChunkPlan;
import org.example.cdc.core.SnapshotCheckpoint;
import org.example.cdc.core.spi.SnapshotProgressStore;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory, thread-safe checkpoint store (including the chunk plan). */
public final class InMemoryProgressStore implements SnapshotProgressStore {

    private final ConcurrentHashMap<String, SnapshotCheckpoint> checkpoints = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ChunkPlan> plans = new ConcurrentHashMap<>();

    @Override
    public Optional<SnapshotCheckpoint> loadCheckpoint(String table) {
        return Optional.ofNullable(checkpoints.get(table));
    }

    @Override
    public synchronized void markCompleted(String table, String chunkId, long snapshotStart) {
        SnapshotCheckpoint current = checkpoints.get(table);
        SnapshotCheckpoint base = current != null
                ? current
                : new SnapshotCheckpoint(table, snapshotStart,
                        Collections.emptySet(), System.currentTimeMillis());
        checkpoints.put(table, base.withCompleted(chunkId));
    }

    @Override
    public void savePlan(String table, long snapshotStart, List<Chunk> plan) {
        plans.put(table, new ChunkPlan(table, snapshotStart, plan));
    }

    @Override
    public Optional<ChunkPlan> loadPlan(String table) {
        return Optional.ofNullable(plans.get(table));
    }

    @Override
    public synchronized void clear(String table) {
        checkpoints.remove(table);
        plans.remove(table);
    }

    public int size() {
        return checkpoints.size();
    }
}
