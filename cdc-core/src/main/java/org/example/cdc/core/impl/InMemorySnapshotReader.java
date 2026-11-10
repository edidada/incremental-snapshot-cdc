package org.example.cdc.core.impl;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.Record;
import org.example.cdc.core.spi.SnapshotReader;

import java.util.List;

/** {@link SnapshotReader} backed by {@link SimulatedDatabase}. */
public final class InMemorySnapshotReader implements SnapshotReader {

    private final SimulatedDatabase db;

    public InMemorySnapshotReader(SimulatedDatabase db) {
        this.db = db;
    }

    @Override
    public List<Record> select(Chunk chunk) {
        return db.select(chunk);
    }
}
