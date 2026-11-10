package org.example.cdc.core.spi;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.SnapshotCheckpoint;
import org.example.cdc.core.SnapshotResult;

/** Observability hooks (metrics, logging, or fault injection in tests). */
public interface SnapshotListener {

    default void onStart(String table, long snapshotStart, int totalChunks) {
    }

    default void onChunkStarted(Chunk chunk) {
    }

    default void onChunkCompleted(Chunk chunk, long baselineRows) {
    }

    default void onCheckpoint(SnapshotCheckpoint checkpoint) {
    }

    default void onComplete(SnapshotResult result) {
    }

    static SnapshotListener noop() {
        return new SnapshotListener() {
        };
    }
}
