package org.example.cdc.core;

import java.util.Comparator;
import java.util.Objects;

/**
 * Identify a table to be snapshotted and, critically, pin the primary-key
 * <em>collation order</em> used everywhere in the pipeline.
 */
public final class TableRef {

    private final String table;
    private final String primaryKeyColumn;
    private final ChunkKeyType keyType;
    private final PrimaryKeyComparator order;

    public TableRef(String table, String primaryKeyColumn,
                    ChunkKeyType keyType, PrimaryKeyComparator order) {
        this.table = Objects.requireNonNull(table, "table");
        this.primaryKeyColumn = Objects.requireNonNull(primaryKeyColumn, "primaryKeyColumn");
        this.keyType = Objects.requireNonNull(keyType, "keyType");
        this.order = Objects.requireNonNull(order, "order");
    }

    public static TableRef numeric(String table, String primaryKeyColumn) {
        return new TableRef(table, primaryKeyColumn, ChunkKeyType.NUMERIC,
                PrimaryKeyComparator.numeric());
    }

    /** Binary string key (e.g. {@code utf8mb4_bin} / Java {@code String.compareTo}). */
    public static TableRef binaryString(String table, String primaryKeyColumn) {
        return new TableRef(table, primaryKeyColumn, ChunkKeyType.STRING,
                PrimaryKeyComparator.binaryString());
    }

    /** Case-insensitive string key (e.g. {@code utf8mb4_general_ci}). */
    public static TableRef caseInsensitiveString(String table, String primaryKeyColumn) {
        return new TableRef(table, primaryKeyColumn, ChunkKeyType.STRING,
                PrimaryKeyComparator.caseInsensitive());
    }

    public String table() {
        return table;
    }

    public String primaryKeyColumn() {
        return primaryKeyColumn;
    }

    public ChunkKeyType keyType() {
        return keyType;
    }

    public Comparator<Object> order() {
        return order;
    }

    @Override
    public String toString() {
        return "TableRef{" + table + "." + primaryKeyColumn + ", " + keyType
                + ", " + order.mode() + '}';
    }
}
