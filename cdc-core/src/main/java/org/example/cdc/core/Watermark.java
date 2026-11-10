package org.example.cdc.core;

import java.util.Objects;

/**
 * The {@code (LOW, HIGH]} binlog window of one chunk. {@code LOW} is captured
 * before the chunk {@code SELECT}, {@code HIGH} after it; every change committed
 * inside the window is replayed to correct/deduplicate the snapshot rows.
 */
public final class Watermark {

    private final String chunkId;
    private final String table;
    private final long low;
    private final long high;

    public Watermark(String chunkId, String table, long low, long high) {
        if (low > high) {
            throw new IllegalArgumentException("low (" + low + ") must be <= high (" + high + ")");
        }
        this.chunkId = Objects.requireNonNull(chunkId, "chunkId");
        this.table = Objects.requireNonNull(table, "table");
        this.low = low;
        this.high = high;
    }

    public boolean contains(long position) {
        return position > low && position <= high;
    }

    public String chunkId() {
        return chunkId;
    }

    public String table() {
        return table;
    }

    public long low() {
        return low;
    }

    public long high() {
        return high;
    }

    @Override
    public String toString() {
        return "Watermark{" + chunkId + ", (" + low + ", " + high + "]}";
    }
}
