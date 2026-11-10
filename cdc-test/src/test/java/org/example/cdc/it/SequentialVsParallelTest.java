package org.example.cdc.it;

import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.SnapshotResult;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.impl.MaterializingSink;
import org.example.cdc.core.impl.SimulatedDatabase;
import org.example.cdc.core.spi.SnapshotListener;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** parallelism=1 (Debezium-like) and parallelism>1 (Flink-like) must agree. */
class SequentialVsParallelTest {

    private static SimulatedDatabase seeded(int rows) {
        TableRef ref = TableRef.numeric("orders", "id");
        SimulatedDatabase db = new SimulatedDatabase(ref);
        for (long id = 1; id <= rows; id++) {
            db.seed(id, Record.row("id", id, "amount", id * 10L, "status", "NEW"));
        }
        return db;
    }

    @Test
    void sequentialAndParallelProduceIdenticalState() {
        SimulatedDatabase sequentialDb = seeded(60);
        SimulatedDatabase parallelDb = seeded(60);

        MaterializingSink seqSink = new MaterializingSink(sequentialDb.ref().order());
        MaterializingSink parSink = new MaterializingSink(parallelDb.ref().order());

        SnapshotResult seq = sequentialDb.coordinator(
                new SnapshotConfig().setChunkSize(7).setParallelism(1)
                        .setResumeFromCheckpoint(false),
                seqSink, SnapshotListener.noop()).execute();
        SnapshotResult par = parallelDb.coordinator(
                new SnapshotConfig().setChunkSize(7).setParallelism(8)
                        .setResumeFromCheckpoint(false),
                parSink, SnapshotListener.noop()).execute();

        assertEquals(seqSink.rows(), parSink.rows());
        assertEquals(60, seqSink.distinctKeys());
        assertEquals(60, parSink.distinctKeys());
        assertEquals(seq.metrics().getChunksProcessed(), par.metrics().getChunksProcessed());
    }
}
