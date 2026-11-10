package org.example.cdc.core.spi;

import org.example.cdc.core.OutputEvent;
import org.example.cdc.core.SnapshotResult;

/**
 * Receives the normalized changelog. Implementations should apply events with
 * last-wins-per-primary-key semantics to materialize the final table state.
 * The coordinator serializes concurrent baseline emissions, but the sink is
 * still expected to be safe under ordered, single-writer delivery.
 */
public interface RecordSink {

    void emit(OutputEvent event);

    /** Number of distinct primary keys currently materialized ({@code -1} if unknown). */
    default int distinctKeys() {
        return -1;
    }

    default void onComplete(SnapshotResult result) {
        // no-op by default
    }
}
