package org.example.cdc.core;

/** Outcome of a completed snapshot run. */
public final class SnapshotResult {

    private final String table;
    private final long snapshotStart;
    private final long snapshotEnd;
    private final int distinctKeys;
    private final SnapshotMetrics metrics;

    public SnapshotResult(String table, long snapshotStart, long snapshotEnd,
                          int distinctKeys, SnapshotMetrics metrics) {
        this.table = table;
        this.snapshotStart = snapshotStart;
        this.snapshotEnd = snapshotEnd;
        this.distinctKeys = distinctKeys;
        this.metrics = metrics;
    }

    public String table() { return table; }
    public long snapshotStart() { return snapshotStart; }
    public long snapshotEnd() { return snapshotEnd; }
    public int distinctKeys() { return distinctKeys; }
    public SnapshotMetrics metrics() { return metrics; }

    @Override
    public String toString() {
        return "SnapshotResult{" + table + ", window=(" + snapshotStart + ", "
                + snapshotEnd + "], distinctKeys=" + distinctKeys + ", " + metrics + '}';
    }
}
