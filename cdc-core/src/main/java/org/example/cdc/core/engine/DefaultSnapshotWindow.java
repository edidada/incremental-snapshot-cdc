package org.example.cdc.core.engine;

import org.example.cdc.core.ChangeEvent;
import org.example.cdc.core.Record;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.Watermark;
import org.example.cdc.core.spi.ChangeLog;
import org.example.cdc.core.spi.SnapshotWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Default DBLog window: a primary-key-keyed buffer plus {@code (LOW, HIGH]}
 * replay. The buffer is a {@link TreeMap} using the table's collation order so
 * that {@link #deduplicate(Object)} and {@link #buffer(Record)} agree with the
 * database's own {@code ORDER BY} even for case-insensitive string keys.
 */
public final class DefaultSnapshotWindow implements SnapshotWindow {

    private enum State { NEW, OPEN, CLOSED }

    private final TableRef table;
    private final ChangeLog changeLog;
    private final NavigableMap<Object, Record> buffer;

    private State state = State.NEW;
    private String chunkId;
    private long low = -1L;
    private long high = -1L;

    public DefaultSnapshotWindow(TableRef table, ChangeLog changeLog) {
        this.table = table;
        this.changeLog = changeLog;
        this.buffer = new TreeMap<>(table.order());
    }

    @Override
    public void open(String chunkId) {
        if (state != State.NEW) {
            throw new IllegalStateException("window already " + state);
        }
        this.chunkId = chunkId;
        this.low = changeLog.currentPosition();
        this.state = State.OPEN;
    }

    @Override
    public void buffer(Record row) {
        requireOpen();
        buffer.put(row.primaryKey(), row);
    }

    @Override
    public void deduplicate(Object primaryKey) {
        if (state == State.NEW) {
            throw new IllegalStateException("window not opened");
        }
        buffer.remove(primaryKey);
    }

    @Override
    public void close(String chunkId) {
        requireOpen();
        this.high = changeLog.currentPosition();
        this.state = State.CLOSED;
    }

    @Override
    public List<ChangeEvent> replay() {
        requireClosed();
        return changeLog.readRange(table.table(), low, high);
    }

    @Override
    public List<Record> flush() {
        requireClosed();
        List<Record> remaining = new ArrayList<>(buffer.values());
        buffer.clear();
        return remaining;
    }

    @Override
    public Watermark watermark() {
        requireClosed();
        return new Watermark(chunkId, table.table(), low, high);
    }

    @Override
    public long low() {
        if (state == State.NEW) {
            throw new IllegalStateException("window not opened");
        }
        return low;
    }

    @Override
    public long high() {
        requireClosed();
        return high;
    }

    @Override
    public int bufferedCount() {
        return buffer.size();
    }

    @Override
    public boolean isOpen() {
        return state == State.OPEN;
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new IllegalStateException("window is not open (state=" + state + ")");
        }
    }

    private void requireClosed() {
        if (state != State.CLOSED) {
            throw new IllegalStateException("window is not closed (state=" + state + ")");
        }
    }
}
