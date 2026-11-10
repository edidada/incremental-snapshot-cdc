package org.example.cdc.core;

import java.util.Objects;

/** A row-level change positioned on the monotonic changelog / binlog. */
public final class ChangeEvent {

    private final long position;
    private final ChangeType type;
    private final String table;
    private final Object primaryKey;
    private final Record after; // null for DELETE
    private final long timestampMillis;

    public ChangeEvent(long position, ChangeType type, String table,
                       Object primaryKey, Record after, long timestampMillis) {
        this.position = position;
        this.type = Objects.requireNonNull(type, "type");
        this.table = Objects.requireNonNull(table, "table");
        this.primaryKey = Objects.requireNonNull(primaryKey, "primaryKey");
        if (type != ChangeType.DELETE && after == null) {
            throw new IllegalArgumentException("after is required for " + type);
        }
        this.after = after;
        this.timestampMillis = timestampMillis;
    }

    public static ChangeEvent insert(long position, String table, Object pk, Record after) {
        return new ChangeEvent(position, ChangeType.INSERT, table, pk, after, System.currentTimeMillis());
    }

    public static ChangeEvent update(long position, String table, Object pk, Record after) {
        return new ChangeEvent(position, ChangeType.UPDATE, table, pk, after, System.currentTimeMillis());
    }

    public static ChangeEvent delete(long position, String table, Object pk) {
        return new ChangeEvent(position, ChangeType.DELETE, table, pk, null, System.currentTimeMillis());
    }

    public long position() {
        return position;
    }

    public ChangeType type() {
        return type;
    }

    public String table() {
        return table;
    }

    public Object primaryKey() {
        return primaryKey;
    }

    public Record after() {
        return after;
    }

    public long timestampMillis() {
        return timestampMillis;
    }

    public boolean isDelete() {
        return type == ChangeType.DELETE;
    }

    @Override
    public String toString() {
        return "ChangeEvent{pos=" + position + ", " + type + ", pk=" + primaryKey + '}';
    }
}
