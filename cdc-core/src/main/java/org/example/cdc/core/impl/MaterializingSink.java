package org.example.cdc.core.impl;

import org.example.cdc.core.OutputEvent;
import org.example.cdc.core.Record;
import org.example.cdc.core.spi.RecordSink;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/** Last-wins-per-key sink that materializes the final table state. */
public final class MaterializingSink implements RecordSink {

    private final NavigableMap<Object, Record> state;
    private final List<OutputEvent> changelog = new ArrayList<>();

    public MaterializingSink(java.util.Comparator<Object> order) {
        this.state = new TreeMap<>(order);
    }

    @Override
    public synchronized void emit(OutputEvent event) {
        changelog.add(event);
        if (event.isDelete()) {
            state.remove(event.primaryKey());
        } else {
            state.put(event.primaryKey(), event.after());
        }
    }

    public synchronized NavigableMap<Object, Record> rows() {
        NavigableMap<Object, Record> copy = new TreeMap<>(state.comparator());
        copy.putAll(state);
        return copy;
    }

    public synchronized Record get(Object pk) {
        return state.get(pk);
    }

    public synchronized List<OutputEvent> changelog() {
        return new ArrayList<>(changelog);
    }

    @Override
    public synchronized int distinctKeys() {
        return state.size();
    }
}
