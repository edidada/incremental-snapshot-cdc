package org.example.cdc.core.spi;

import org.example.cdc.core.ChangeEvent;
import org.example.cdc.core.Record;
import org.example.cdc.core.Watermark;

import java.util.List;

/**
 * The DBLog snapshot window for a single chunk.
 *
 * <p>Lifecycle: {@link #open(String)} records LOW, the chunk {@code SELECT} rows
 * are {@link #buffer(Record) buffered}, {@link #close(String)} records HIGH,
 * every binlog event in {@code (LOW, HIGH]} is {@link #deduplicate(Object) removed}
 * from the buffer (the log wins over the snapshot), and {@link #flush()} emits
 * the remaining rows. This is the algorithm core described in the project README.</p>
 */
public interface SnapshotWindow {

    void open(String chunkId);

    void buffer(Record row);

    void deduplicate(Object primaryKey);

    void close(String chunkId);

    /** Replay the change events inside {@code (LOW, HIGH]} for this table. */
    List<ChangeEvent> replay();

    /** Return and clear the buffered rows that were not superseded by log events. */
    List<Record> flush();

    Watermark watermark();

    long low();

    long high();

    int bufferedCount();

    boolean isOpen();
}
