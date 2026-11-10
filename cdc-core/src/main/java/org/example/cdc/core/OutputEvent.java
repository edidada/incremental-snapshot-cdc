package org.example.cdc.core;

import java.util.Comparator;
import java.util.Objects;

/**
 * A normalized event emitted to the {@link org.example.cdc.core.spi.RecordSink}.
 *
 * <p>Two kinds of events reach the sink:</p>
 * <ul>
 *   <li><b>BASELINE</b> - snapshot rows that survived the DBLog window dedup.
 *       They represent the table state as of the chunk's LOW watermark.</li>
 *   <li><b>STREAM</b> - binlog changes in {@code (snapshotStart, snapshotEnd]}
 *       replayed in position order after the snapshot phase.</li>
 * </ul>
 * A last-wins-per-key materializer therefore reconstructs the exact table state
 * as of {@code snapshotEnd}: stream events carry positions strictly greater than
 * any baseline and win over them.
 */
public final class OutputEvent {

    public enum Kind { UPSERT, DELETE }

    public static final int BASELINE_PRIORITY = 0;
    public static final int STREAM_PRIORITY = 1;

    /** Ordering used to produce a deterministic changelog for external consumers. */
    public static final Comparator<OutputEvent> ORDER =
            Comparator.comparingLong(OutputEvent::position)
                    .thenComparingInt(OutputEvent::priority);

    private final long position;
    private final int priority;
    private final Kind kind;
    private final Object primaryKey;
    private final Record after; // null for DELETE
    private final String source;

    private OutputEvent(long position, int priority, Kind kind,
                        Object primaryKey, Record after, String source) {
        this.position = position;
        this.priority = priority;
        this.kind = kind;
        this.primaryKey = Objects.requireNonNull(primaryKey, "primaryKey");
        this.after = after;
        this.source = source;
    }

    public static OutputEvent baseline(long lowWatermark, Record row) {
        return new OutputEvent(lowWatermark, BASELINE_PRIORITY, Kind.UPSERT,
                row.primaryKey(), row, "baseline");
    }

    public static OutputEvent fromChangeEvent(ChangeEvent event) {
        return new OutputEvent(event.position(), STREAM_PRIORITY,
                event.isDelete() ? Kind.DELETE : Kind.UPSERT,
                event.primaryKey(), event.after(), "stream");
    }

    public long position() {
        return position;
    }

    public int priority() {
        return priority;
    }

    public Kind kind() {
        return kind;
    }

    public Object primaryKey() {
        return primaryKey;
    }

    public Record after() {
        return after;
    }

    public String source() {
        return source;
    }

    public boolean isDelete() {
        return kind == Kind.DELETE;
    }

    @Override
    public String toString() {
        return "OutputEvent{pos=" + position + ", " + kind + ", pk=" + primaryKey
                + ", src=" + source + '}';
    }
}
