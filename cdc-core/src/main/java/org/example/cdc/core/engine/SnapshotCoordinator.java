package org.example.cdc.core.engine;

import org.example.cdc.core.ChangeEvent;
import org.example.cdc.core.Chunk;
import org.example.cdc.core.ChunkPlan;
import org.example.cdc.core.IncrementalSnapshotException;
import org.example.cdc.core.OutputEvent;
import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotCheckpoint;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.SnapshotMetrics;
import org.example.cdc.core.SnapshotResult;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.spi.ChangeLog;
import org.example.cdc.core.spi.ChunkSplitter;
import org.example.cdc.core.spi.RecordSink;
import org.example.cdc.core.spi.SnapshotListener;
import org.example.cdc.core.spi.SnapshotProgressStore;
import org.example.cdc.core.spi.SnapshotReader;
import org.example.cdc.core.spi.SnapshotWindow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Incremental (DBLog-style) snapshot coordinator.
 *
 * <h2>Algorithm</h2>
 * <ol>
 *   <li>Split the table into primary-key chunks.</li>
 *   <li>For each chunk (possibly in parallel): open a {@link SnapshotWindow}
 *       at LOW, {@code SELECT} the chunk into the window buffer, close at HIGH,
 *       replay {@code (LOW, HIGH]} binlog events and drop every buffered row
 *       whose key appears in that window (log wins). The remaining rows are the
 *       chunk <em>baseline</em>, emitted to the sink immediately and checkpointed.</li>
 *   <li>After all chunks, replay the single authoritative changelog over the
 *       whole {@code (snapshotStart, snapshotEnd]} window in position order.</li>
 * </ol>
 *
 * <p>Why a global stream instead of emitting each chunk's window events? With
 * parallel chunks the {@code (LOW, HIGH]} windows overlap in time, so unioning
 * them would duplicate changes. Debezium avoids this by processing chunks
 * <em>sequentially</em> (windows tile the timeline); a Flink-CDC-style parallel
 * engine needs one unified stream. Routing every change through a single
 * ordered changelog makes the emitted result correct regardless of parallelism,
 * while a last-wins-per-key sink reconstructs the exact table state as of
 * {@code snapshotEnd}. Baselines carry their LOW position (lower priority), so
 * any later change overrides them.</p>
 *
 * <h2>Recovery</h2>
 * The chunk plan (with its original {@code snapshotStart}) is persisted at the
 * beginning of a run, and completed chunk ids are checkpointed as chunks finish.
 * On restart the coordinator reuses the <em>same</em> boundaries and start
 * position, skips completed chunks (their baselines are already in the sink),
 * and resumes the rest. Persisting the plan - rather than re-splitting from a
 * table that may have changed since the crash - is essential: recomputed
 * boundaries would shift and silently duplicate or lose rows. This mirrors how
 * Flink CDC keeps chunk ranges in its checkpointed operator state.</p>
 */
public final class SnapshotCoordinator {

    private static final Logger LOG = Logger.getLogger(SnapshotCoordinator.class.getName());

    private final TableRef table;
    private final ChunkSplitter splitter;
    private final SnapshotReader reader;
    private final ChangeLog changeLog;
    private final SnapshotProgressStore progressStore;
    private final RecordSink sink;
    private final SnapshotConfig config;
    private final SnapshotListener listener;

    public SnapshotCoordinator(TableRef table,
                               ChunkSplitter splitter,
                               SnapshotReader reader,
                               ChangeLog changeLog,
                               SnapshotProgressStore progressStore,
                               RecordSink sink,
                               SnapshotConfig config,
                               SnapshotListener listener) {
        this.table = table;
        this.splitter = splitter;
        this.reader = reader;
        this.changeLog = changeLog;
        this.progressStore = progressStore;
        this.sink = sink;
        this.config = config;
        this.listener = listener;
    }

    public SnapshotResult execute() {
        long started = System.nanoTime();
        SnapshotMetrics metrics = new SnapshotMetrics();
        metrics.setParallelism(config.getParallelism());

        Optional<SnapshotCheckpoint> existing = config.isResumeFromCheckpoint()
                ? progressStore.loadCheckpoint(table.table())
                : Optional.empty();
        Optional<ChunkPlan> savedPlan = config.isResumeFromCheckpoint()
                ? progressStore.loadPlan(table.table())
                : Optional.empty();

        long snapshotStart;
        List<Chunk> chunks;
        if (savedPlan.isPresent()) {
            // Resume: reuse the exact original boundaries and start position.
            snapshotStart = savedPlan.get().snapshotStart();
            chunks = savedPlan.get().chunks();
        } else {
            snapshotStart = existing
                    .map(SnapshotCheckpoint::snapshotStart)
                    .orElseGet(changeLog::currentPosition);
            chunks = splitter.split(table, config.getChunkSize());
            if (config.isResumeFromCheckpoint()) {
                progressStore.savePlan(table.table(), snapshotStart, chunks);
            }
        }
        Set<String> completed = existing
                .map(c -> new HashSet<>(c.completedChunkIds()))
                .orElseGet(HashSet::new);

        metrics.addChunksTotal(chunks.size());
        listener.onStart(table.table(), snapshotStart, chunks.size());
        LOG.info(() -> "Starting snapshot of " + table + " at " + snapshotStart
                + " with " + chunks.size() + " chunks (parallelism=" + config.getParallelism()
                + ", resume=" + !completed.isEmpty() + ")");

        ExecutorService pool = Executors.newFixedThreadPool(
                config.getParallelism(), namedThreadFactory("cdc-chunk"));
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (Chunk chunk : chunks) {
                if (completed.contains(chunk.id())) {
                    metrics.incChunksSkipped();
                    continue;
                }
                futures.add(pool.submit(() -> runChunk(chunk, snapshotStart, metrics)));
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new IncrementalSnapshotException("chunk worker failed", cause);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IncrementalSnapshotException("snapshot interrupted", e);
                }
            }
        } finally {
            pool.shutdownNow();
            try {
                pool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Streaming phase: one ordered changelog covering the whole snapshot window.
        long snapshotEnd = changeLog.currentPosition();
        List<ChangeEvent> stream = changeLog.readRange(table.table(), snapshotStart, snapshotEnd);
        stream.sort(Comparator.comparingLong(ChangeEvent::position));
        for (ChangeEvent event : stream) {
            sink.emit(OutputEvent.fromChangeEvent(event));
            metrics.incStreamEvents();
        }

        progressStore.clear(table.table());

        metrics.setDurationMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        SnapshotResult result = new SnapshotResult(table.table(), snapshotStart,
                snapshotEnd, sink.distinctKeys(), metrics);
        sink.onComplete(result);
        listener.onComplete(result);
        LOG.info(() -> "Snapshot complete: " + result);
        return result;
    }

    private void runChunk(Chunk chunk, long snapshotStart, SnapshotMetrics metrics) {
        listener.onChunkStarted(chunk);
        List<OutputEvent> baselines = processChunkWithRetry(chunk, metrics);

        // Serialize emissions so any sink works with parallel chunk workers.
        synchronized (sink) {
            for (OutputEvent event : baselines) {
                sink.emit(event);
            }
        }
        metrics.addBaselineRows(baselines.size());

        progressStore.markCompleted(table.table(), chunk.id(), snapshotStart);
        metrics.incChunksProcessed();

        SnapshotCheckpoint checkpoint = progressStore.loadCheckpoint(table.table())
                .orElseThrow(() -> new IncrementalSnapshotException(
                        "checkpoint vanished after markCompleted for " + chunk.id()));
        listener.onCheckpoint(checkpoint);
        listener.onChunkCompleted(chunk, baselines.size());
    }

    private List<OutputEvent> processChunkWithRetry(Chunk chunk, SnapshotMetrics metrics) {
        int attempt = 0;
        while (true) {
            try {
                return processChunk(chunk, metrics);
            } catch (RuntimeException ex) {
                if (attempt >= config.getMaxRetries()) {
                    throw ex;
                }
                attempt++;
                metrics.incRetries();
                LOG.log(Level.WARNING, "Chunk " + chunk.id() + " failed (attempt "
                        + attempt + "/" + config.getMaxRetries() + "), retrying", ex);
                if (config.getRetryBackoffMillis() > 0) {
                    try {
                        Thread.sleep(config.getRetryBackoffMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IncrementalSnapshotException("retry interrupted", ie);
                    }
                }
            }
        }
    }

    private List<OutputEvent> processChunk(Chunk chunk, SnapshotMetrics metrics) {
        SnapshotWindow window = new DefaultSnapshotWindow(table, changeLog);
        window.open(chunk.id());

        List<Record> rows = reader.select(chunk);
        for (Record row : rows) {
            window.buffer(row);
        }
        window.close(chunk.id());

        // DBLog correction: replay (LOW, HIGH], drop buffered rows touched by the log.
        for (ChangeEvent event : window.replay()) {
            if (chunk.contains(event.primaryKey())) {
                window.deduplicate(event.primaryKey());
                metrics.addWindowEventsReplayed(1);
            }
        }

        List<Record> baseline = window.flush();
        long low = window.low();
        List<OutputEvent> out = new ArrayList<>(baseline.size());
        for (Record row : baseline) {
            out.add(OutputEvent.baseline(low, row));
        }
        return out;
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return runnable -> {
            Thread t = new Thread(runnable, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
