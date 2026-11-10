package org.example.cdc.core.impl;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.ChangeType;
import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.engine.SnapshotCoordinator;
import org.example.cdc.core.spi.ChunkSplitter;
import org.example.cdc.core.spi.RecordSink;
import org.example.cdc.core.spi.SnapshotListener;
import org.example.cdc.core.spi.SnapshotProgressStore;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Consumer;

/**
 * A JDK-only, fully concurrent in-memory source that wires a table, a binlog,
 * a chunk reader and a splitter together. It is a shipped reference
 * implementation (usable by demos, benchmarks and downstream tests), not just
 * test scaffolding.
 *
 * <p>{@link #setSelectHook(Consumer)} lets a caller inject writes <em>inside</em>
 * a chunk's SELECT window, deterministically reproducing concurrent DML without
 * any timing flakiness.</p>
 */
public final class SimulatedDatabase {

    private final TableRef ref;
    private final InMemoryTable table;
    private final InMemoryChangeLog changeLog;
    private volatile Consumer<Chunk> selectHook;

    public SimulatedDatabase(TableRef ref) {
        this.ref = ref;
        this.table = new InMemoryTable(ref);
        this.changeLog = new InMemoryChangeLog();
    }

    public TableRef ref() {
        return ref;
    }

    public InMemoryTable table() {
        return table;
    }

    public InMemoryChangeLog changeLog() {
        return changeLog;
    }

    public void setSelectHook(Consumer<Chunk> selectHook) {
        this.selectHook = selectHook;
    }

    /** Initial load: put a row without generating a changelog event. */
    public synchronized void seed(Object pk, Map<String, Object> values) {
        table.put(pk, new Record(ref.table(), pk, values));
    }

    public synchronized long insert(Object pk, Map<String, Object> values) {
        Record record = new Record(ref.table(), pk, values);
        table.put(pk, record);
        return changeLog.append(ref.table(), ChangeType.INSERT, pk, record);
    }

    public synchronized long update(Object pk, Map<String, Object> values) {
        Record record = new Record(ref.table(), pk, values);
        table.put(pk, record);
        return changeLog.append(ref.table(), ChangeType.UPDATE, pk, record);
    }

    public synchronized long delete(Object pk) {
        table.remove(pk);
        return changeLog.append(ref.table(), ChangeType.DELETE, pk, null);
    }

    /** Chunk SELECT; the optional hook fires before the rows are returned. */
    public List<Record> select(Chunk chunk) {
        List<Record> rows = table.selectRange(chunk);
        Consumer<Chunk> hook = selectHook;
        if (hook != null) {
            hook.accept(chunk);
        }
        return rows;
    }

    public List<Object> sortedPrimaryKeys() {
        return table.sortedPrimaryKeys();
    }

    public NavigableMap<Object, Record> currentRows() {
        return table.snapshot();
    }

    public int size() {
        return table.size();
    }

    /** Fully wired coordinator with the default in-memory components. */
    public SnapshotCoordinator coordinator(SnapshotConfig config, RecordSink sink,
                                           SnapshotListener listener) {
        return coordinator(config, sink, listener,
                new InMemoryProgressStore(), new RangeChunkSplitter(this));
    }

    /** Fully wired coordinator allowing a shared progress store / custom splitter (recovery tests). */
    public SnapshotCoordinator coordinator(SnapshotConfig config, RecordSink sink,
                                           SnapshotListener listener,
                                           SnapshotProgressStore progressStore,
                                           ChunkSplitter splitter) {
        return new SnapshotCoordinator(ref, splitter, new InMemorySnapshotReader(this),
                changeLog, progressStore, sink, config, listener);
    }
}
