package org.example.cdc.core;

/** Counters collected during a snapshot run (exposed via {@link SnapshotResult}). */
public final class SnapshotMetrics {

    private int chunksTotal;
    private int chunksProcessed;
    private int chunksSkipped;
    private long baselineRows;
    private long streamEvents;
    private long windowEventsReplayed;
    private int retries;
    private int parallelism;
    private long durationMillis;

    public synchronized void addChunksTotal(int n) { chunksTotal += n; }
    public synchronized void incChunksProcessed() { chunksProcessed++; }
    public synchronized void incChunksSkipped() { chunksSkipped++; }
    public synchronized void addBaselineRows(long n) { baselineRows += n; }
    public synchronized void incStreamEvents() { streamEvents++; }
    public synchronized void addWindowEventsReplayed(long n) { windowEventsReplayed += n; }
    public synchronized void incRetries() { retries++; }

    public int getChunksTotal() { return chunksTotal; }
    public int getChunksProcessed() { return chunksProcessed; }
    public int getChunksSkipped() { return chunksSkipped; }
    public long getBaselineRows() { return baselineRows; }
    public long getStreamEvents() { return streamEvents; }
    public long getWindowEventsReplayed() { return windowEventsReplayed; }
    public int getRetries() { return retries; }
    public int getParallelism() { return parallelism; }
    public long getDurationMillis() { return durationMillis; }

    public void setParallelism(int parallelism) { this.parallelism = parallelism; }
    public void setDurationMillis(long durationMillis) { this.durationMillis = durationMillis; }

    @Override
    public String toString() {
        return "SnapshotMetrics{chunks=" + chunksProcessed + "/" + chunksTotal
                + " (skipped=" + chunksSkipped + "), baselineRows=" + baselineRows
                + ", streamEvents=" + streamEvents
                + ", windowReplay=" + windowEventsReplayed
                + ", retries=" + retries
                + ", parallelism=" + parallelism
                + ", durationMs=" + durationMillis + '}';
    }
}
