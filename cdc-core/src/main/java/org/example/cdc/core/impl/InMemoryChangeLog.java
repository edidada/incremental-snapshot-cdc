package org.example.cdc.core.impl;

import org.example.cdc.core.ChangeEvent;
import org.example.cdc.core.ChangeType;
import org.example.cdc.core.Record;
import org.example.cdc.core.spi.ChangeLog;

import java.util.ArrayList;
import java.util.List;

/** Thread-safe, append-only in-memory binlog with monotonic positions. */
public final class InMemoryChangeLog implements ChangeLog {

    private final List<ChangeEvent> events = new ArrayList<>();
    private long position = 0L;

    public synchronized long append(String table, ChangeType type, Object pk, Record after) {
        position++;
        ChangeEvent event = new ChangeEvent(position, type, table, pk, after,
                System.currentTimeMillis());
        events.add(event);
        return position;
    }

    @Override
    public synchronized long currentPosition() {
        return position;
    }

    @Override
    public synchronized List<ChangeEvent> readRange(String table, long lowExclusive, long highInclusive) {
        List<ChangeEvent> out = new ArrayList<>();
        for (ChangeEvent event : events) {
            if (event.position() > lowExclusive
                    && event.position() <= highInclusive
                    && event.table().equals(table)) {
                out.add(event);
            }
        }
        return out; // already in ascending position order
    }

    public synchronized int size() {
        return events.size();
    }
}
