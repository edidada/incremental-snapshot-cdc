package org.example.cdc.core.spi;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.Record;

import java.util.List;

/** Executes the chunk {@code SELECT} against the source database. */
public interface SnapshotReader {

    /** Read all rows whose primary key falls inside the chunk range. */
    List<Record> select(Chunk chunk);
}
