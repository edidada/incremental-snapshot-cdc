package org.example.cdc.connector.mysql;

import org.example.cdc.core.ChangeEvent;
import org.example.cdc.core.ChangeType;
import org.example.cdc.core.Chunk;
import org.example.cdc.core.ChunkIds;
import org.example.cdc.core.ChunkPlan;
import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotCheckpoint;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.SnapshotResult;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.impl.MaterializingSink;
import org.example.cdc.core.spi.SnapshotListener;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the real JDBC implementations against embedded H2 in MySQL mode. */
class JdbcConnectorTest {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    private DataSource dataSource;
    private TableRef ref;
    private JdbcSnapshotEnvironment env;

    @BeforeEach
    void setUp() throws SQLException {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:cdc_jdbc_" + DB_SEQ.incrementAndGet()
                + ";MODE=MySQL;DATABASE_TO_UPPER=FALSE;DB_CLOSE_DELAY=-1");
        this.dataSource = h2;
        this.ref = TableRef.numeric("orders", "id");
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE orders ("
                    + "id BIGINT PRIMARY KEY, amount BIGINT, status VARCHAR(64))");
        }
        this.env = new JdbcSnapshotEnvironment(dataSource, ref);
        this.env.initializeSchema();
        for (long id = 1; id <= 40; id++) {
            env.apply(ChangeType.INSERT, id,
                    Record.row("id", id, "amount", id * 10L, "status", "NEW"));
        }
    }

    @Test
    void samplingSplitterProducesAnExactPartition() {
        List<Chunk> chunks = env.splitter().split(ref, 6);
        assertEquals(7, chunks.size()); // ceil(40/6)

        Set<Object> seen = new HashSet<>();
        int rows = 0;
        for (Chunk chunk : chunks) {
            for (Record record : env.reader().select(chunk)) {
                rows++;
                assertTrue(seen.add(record.primaryKey()), "duplicate key across chunks");
            }
        }
        assertEquals(40, rows);
    }

    @Test
    void fullSnapshotOverJdbcMatchesSource() throws SQLException {
        MaterializingSink sink = new MaterializingSink(ref.order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(6)
                .setParallelism(4)
                .setResumeFromCheckpoint(false);

        SnapshotResult result = env.coordinator(config, sink, SnapshotListener.noop()).execute();

        assertEquals(readSource(), sink.rows());
        assertEquals(40, sink.distinctKeys());
        assertEquals(7, result.metrics().getChunksProcessed());
        assertEquals(0, result.metrics().getStreamEvents());
    }

    @Test
    void concurrentDmlThroughChangelogIsReconciled() throws SQLException {
        AtomicBoolean once = new AtomicBoolean(false);
        env.reader().setSelectHook(chunk -> {
            if (once.compareAndSet(false, true)) {
                env.apply(ChangeType.UPDATE, 3L,
                        Record.row("id", 3L, "amount", 999L, "status", "UPDATED"));
                env.apply(ChangeType.DELETE, 7L, Collections.emptyMap());
                env.apply(ChangeType.INSERT, 41L,
                        Record.row("id", 41L, "amount", 410L, "status", "INSERTED"));
            }
        });

        MaterializingSink sink = new MaterializingSink(ref.order());
        SnapshotConfig config = new SnapshotConfig()
                .setChunkSize(8)
                .setParallelism(4)
                .setResumeFromCheckpoint(false);
        env.coordinator(config, sink, SnapshotListener.noop()).execute();

        assertEquals(readSource(), sink.rows());
        assertEquals(40, sink.distinctKeys()); // 40 - 1 delete + 1 insert
        assertEquals("UPDATED", sink.get(3L).get("status"));
        assertNull(sink.get(7L));
        assertNotNull(sink.get(41L));
    }

    @Test
    void changelogWindowAndProgressStoreRoundTrip() {
        long base = env.changeLog().currentPosition();
        long p1 = env.changeLog().append("orders", ChangeType.INSERT, 100L,
                Record.of("orders", 100L, "id", 100L, "amount", 1L, "status", "X"));

        assertEquals(0, env.changeLog().readRange("orders", base, base).size());
        List<ChangeEvent> window = env.changeLog().readRange("orders", base, p1);
        assertEquals(1, window.size());
        assertEquals(100L, window.get(0).primaryKey());
        assertEquals(base, env.changeLog().readRange("orders", 0, base).size());

        env.progressStore().markCompleted("orders", ChunkIds.format("orders", 0), 5L);
        env.progressStore().markCompleted("orders", ChunkIds.format("orders", 1), 5L);
        SnapshotCheckpoint checkpoint = env.progressStore().loadCheckpoint("orders").orElseThrow();
        assertEquals(5L, checkpoint.snapshotStart());
        assertEquals(2, checkpoint.completedChunkIds().size());

        env.progressStore().clear("orders");
        assertTrue(env.progressStore().loadCheckpoint("orders").isEmpty());
    }

    @Test
    void chunkPlanRoundTripsThroughJdbc() {
        List<Chunk> plan = env.splitter().split(ref, 6);
        env.progressStore().savePlan("orders", 42L, plan);

        ChunkPlan loaded = env.progressStore().loadPlan("orders").orElseThrow();
        assertEquals(42L, loaded.snapshotStart());
        assertEquals(plan.size(), loaded.chunks().size());
        for (int i = 0; i < plan.size(); i++) {
            assertEquals(plan.get(i).id(), loaded.chunks().get(i).id());
            assertEquals(plan.get(i).lower(), loaded.chunks().get(i).lower());
            assertEquals(plan.get(i).upper(), loaded.chunks().get(i).upper());
            assertEquals(plan.get(i).includeUpper(), loaded.chunks().get(i).includeUpper());
        }

        env.progressStore().clear("orders");
        assertTrue(env.progressStore().loadPlan("orders").isEmpty());
    }

    /** Read the source table into the same normalized shape as the sink. */
    private NavigableMap<Object, Record> readSource() throws SQLException {
        NavigableMap<Object, Record> map = new TreeMap<>(ref.order());
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM " + ref.table())) {
            ResultSetMetaData meta = rs.getMetaData();
            int pkIndex = -1;
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if (meta.getColumnLabel(i).equalsIgnoreCase(ref.primaryKeyColumn())) {
                    pkIndex = i;
                }
            }
            while (rs.next()) {
                Map<String, Object> values = new LinkedHashMap<>();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    values.put(meta.getColumnLabel(i).toLowerCase(Locale.ROOT), rs.getObject(i));
                }
                Object key = KeyCodec.canonicalize(rs.getObject(pkIndex), ref.keyType());
                map.put(key, new Record(ref.table(), key, values));
            }
        }
        return map;
    }
}
