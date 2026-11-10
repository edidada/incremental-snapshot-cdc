package org.example.cdc.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** An immutable materialized row: its primary key plus the full column map. */
public final class Record {

    private final String table;
    private final Object primaryKey;
    private final Map<String, Object> values;

    public Record(String table, Object primaryKey, Map<String, Object> values) {
        this.table = Objects.requireNonNull(table, "table");
        this.primaryKey = Objects.requireNonNull(primaryKey, "primaryKey");
        this.values = Collections.unmodifiableMap(
                new LinkedHashMap<>(Objects.requireNonNull(values, "values")));
    }

    /** Convenience factory: {@code of("orders", 7L, "id", 7L, "amount", 100L)}. */
    public static Record of(String table, Object primaryKey, Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues must be an even number of arguments");
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return new Record(table, primaryKey, values);
    }

    public static Map<String, Object> row(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues must be an even number of arguments");
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return values;
    }

    public String table() {
        return table;
    }

    public Object primaryKey() {
        return primaryKey;
    }

    public Map<String, Object> values() {
        return values;
    }

    public Object get(String column) {
        return values.get(column);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Record)) {
            return false;
        }
        Record other = (Record) o;
        return table.equals(other.table)
                && primaryKey.equals(other.primaryKey)
                && values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, primaryKey, values);
    }

    @Override
    public String toString() {
        return "Record{" + table + ", pk=" + primaryKey + ", values=" + values + '}';
    }
}
