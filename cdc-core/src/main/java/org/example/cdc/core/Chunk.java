package org.example.cdc.core;

import java.util.Comparator;
import java.util.Objects;

/**
 * A half-open primary-key range {@code [lower, upper)} (the last chunk is
 * unbounded above). A {@code null} bound means unbounded on that side.
 *
 * <p>{@link #contains(Object)} uses the table's collation comparator, so chunk
 * membership is always evaluated with the same order that produced the
 * boundaries. This is what keeps string-key splitting collation-safe.</p>
 */
public final class Chunk {

    private final String id;
    private final String table;
    private final String primaryKeyColumn;
    private final Object lower;
    private final Object upper;
    private final boolean includeUpper;
    private final int index;
    private final int total;
    private final Comparator<Object> order;

    public Chunk(String id, String table, String primaryKeyColumn,
                 Object lower, Object upper, boolean includeUpper,
                 int index, int total, Comparator<Object> order) {
        this.id = Objects.requireNonNull(id, "id");
        this.table = Objects.requireNonNull(table, "table");
        this.primaryKeyColumn = Objects.requireNonNull(primaryKeyColumn, "primaryKeyColumn");
        this.lower = lower;
        this.upper = upper;
        this.includeUpper = includeUpper;
        this.index = index;
        this.total = total;
        this.order = Objects.requireNonNull(order, "order");
    }

    public boolean contains(Object primaryKey) {
        if (lower != null && order.compare(primaryKey, lower) < 0) {
            return false;
        }
        if (upper == null) {
            return true;
        }
        int cmp = order.compare(primaryKey, upper);
        return includeUpper ? cmp <= 0 : cmp < 0;
    }

    public String id() {
        return id;
    }

    public String table() {
        return table;
    }

    public String primaryKeyColumn() {
        return primaryKeyColumn;
    }

    public Object lower() {
        return lower;
    }

    public Object upper() {
        return upper;
    }

    public boolean includeUpper() {
        return includeUpper;
    }

    public int index() {
        return index;
    }

    public int total() {
        return total;
    }

    public Comparator<Object> order() {
        return order;
    }

    @Override
    public String toString() {
        return "Chunk{" + id + " [" + lower + " .. " + upper
                + (includeUpper ? "]" : ")") + "}";
    }
}
