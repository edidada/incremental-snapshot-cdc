package org.example.cdc.core;

/** Tunables of a snapshot run. */
public final class SnapshotConfig {

    private int chunkSize = 1024;
    private int parallelism = 4;
    private int maxRetries = 3;
    private long retryBackoffMillis = 25L;
    private boolean resumeFromCheckpoint = true;

    public SnapshotConfig() {
    }

    /** Max rows per chunk. */
    public int getChunkSize() {
        return chunkSize;
    }

    public SnapshotConfig setChunkSize(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
        return this;
    }

    /**
     * Number of chunk worker threads. {@code 1} reproduces Debezium's sequential
     * single-task behaviour; {@code >1} is the Flink-CDC-style parallel model.
     */
    public int getParallelism() {
        return parallelism;
    }

    public SnapshotConfig setParallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0");
        }
        this.parallelism = parallelism;
        return this;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public SnapshotConfig setMaxRetries(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        this.maxRetries = maxRetries;
        return this;
    }

    public long getRetryBackoffMillis() {
        return retryBackoffMillis;
    }

    public SnapshotConfig setRetryBackoffMillis(long retryBackoffMillis) {
        this.retryBackoffMillis = Math.max(0L, retryBackoffMillis);
        return this;
    }

    public boolean isResumeFromCheckpoint() {
        return resumeFromCheckpoint;
    }

    public SnapshotConfig setResumeFromCheckpoint(boolean resumeFromCheckpoint) {
        this.resumeFromCheckpoint = resumeFromCheckpoint;
        return this;
    }
}
