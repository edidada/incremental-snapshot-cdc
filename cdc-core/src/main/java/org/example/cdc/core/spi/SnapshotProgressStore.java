package org.example.cdc.core.spi;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.ChunkPlan;
import org.example.cdc.core.SnapshotCheckpoint;

import java.util.List;
import java.util.Optional;

/**
 * Durable snapshot progress. Implementations must be safe to call from multiple
 * chunk worker threads concurrently.
 */
public interface SnapshotProgressStore {

    Optional<SnapshotCheckpoint> loadCheckpoint(String table);

    /** Persist that a chunk completed, together with the run's original start position. */
    void markCompleted(String table, String chunkId, long snapshotStart);

    /**
     * Persist the chunk plan and original start position at the beginning of a
     * run. Optional: stores that do not support it keep the default no-op and
     * the coordinator falls back to re-splitting on resume.
     */
    default void savePlan(String table, long snapshotStart, List<Chunk> plan) {
    }

    /** Load a previously persisted chunk plan. */
    default Optional<ChunkPlan> loadPlan(String table) {
        return Optional.empty();
    }

    /** Drop all progress for a table (called after a successful full snapshot). */
    void clear(String table);
}
