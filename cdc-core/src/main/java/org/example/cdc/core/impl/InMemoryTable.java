package org.example.cdc.core.impl;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.Record;
import org.example.cdc.core.TableRef;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * In-memory table whose rows are ordered by the configured collation. Range
 * SELECTs evaluate boundaries with that same collation (exactly what a DB
 * {@code WHERE pk >= ? AND pk < ?} does), independent of the chunk object.
 */
public final class InMemoryTable {

    private final TableRef ref;
    private final NavigableMap<Object, Record> rows;

    public InMemoryTable(TableRef ref) {
        this.ref = ref;
        this.rows = new TreeMap<>(ref.order());
    }

    public synchronized void put(Object pk, Record record) {
        rows.put(pk, record);
    }

    public synchronized void remove(Object pk) {
        rows.remove(pk);
    }

    public synchronized Record get(Object pk) {
        return rows.get(pk);
    }

    public synchronized int size() {
        return rows.size();
    }

    public synchronized List<Object> sortedPrimaryKeys() {
        return new ArrayList<>(rows.navigableKeySet());
    }

    public synchronized List<Record> allRows() {
        return new ArrayList<>(rows.values());
    }

    public synchronized NavigableMap<Object, Record> snapshot() {
        NavigableMap<Object, Record> copy = new TreeMap<>(ref.order());
        copy.putAll(rows);
        return copy;
    }

    /** Evaluate the chunk range using the table's own (DB) collation. */
    public synchronized List<Record> selectRange(Chunk chunk) {
        Comparator<Object> order = ref.order();
        List<Record> out = new ArrayList<>();
        for (Object pk : rows.navigableKeySet()) {
            if (chunk.lower() != null && order.compare(pk, chunk.lower()) < 0) {
                continue;
            }
            if (chunk.upper() != null) {
                int cmp = order.compare(pk, chunk.upper());
                if (chunk.includeUpper() ? cmp > 0 : cmp >= 0) {
                    continue;
                }
            }
            out.add(rows.get(pk));
        }
        return out;
    }
}
