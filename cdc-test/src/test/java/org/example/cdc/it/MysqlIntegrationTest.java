package org.example.cdc.it;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.example.cdc.core.ChangeType;
import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.TableRef;
import org.example.cdc.connector.mysql.JdbcSnapshotEnvironment;
import org.example.cdc.connector.mysql.KeyCodec;
import org.example.cdc.connector.mysql.JdbcSnapshotProgressStore;
import org.example.cdc.connector.mysql.JdbcChangeLog;
import org.example.cdc.core.impl.MaterializingSink;
import org.example.cdc.core.spi.SnapshotListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * End-to-end test against a real MySQL 8 instance (the connector's JDBC code,
 * not the in-memory reference implementation).
 *
 * <p>Opt-in: only runs when {@code MYSQL_URL} is set. The GitHub Actions
 * {@code mysql-integration} job provides a MySQL service and these variables;
 * ordinary CI builds skip it so they stay fast and deterministic.</p>
 */
@EnabledIfEnvironmentVariable(named = "MYSQL_URL", matches = ".+")
class MysqlIntegrationTest {

    @Test
    void realMysqlEndToEndSnapshotWithConcurrentDml() throws SQLException {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(System.getenv("MYSQL_URL"));
        ds.setUser(System.getenv().getOrDefault("MYSQL_USER", "root"));
        ds.setPassword(System.getenv().getOrDefault("MYSQL_PASSWORD", "root"));

        TableRef ref = TableRef.numeric("orders", "id");
        resetSchema(ds);

        JdbcSnapshotEnvironment env = new JdbcSnapshotEnvironment(ds, ref);
        env.initializeSchema();
        for (long id = 1; id <= 40; id++) {
            env.apply(ChangeType.INSERT, id,
                    Record.row("id", id, "amount", id * 10L, "status", "NEW"));
        }

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
                .setChunkSize(6)
                .setParallelism(4)
                .setResumeFromCheckpoint(false);
        env.coordinator(config, sink, SnapshotListener.noop()).execute();

        assertEquals(readSource(ds, ref), sink.rows());
        assertEquals("UPDATED", sink.get(3L).get("status"));
        assertNull(sink.get(7L));
        assertNotNull(sink.get(41L));
    }

    private static void resetSchema(DataSource ds) throws SQLException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("DROP TABLE IF EXISTS orders");
            s.executeUpdate("DROP TABLE IF EXISTS " + JdbcChangeLog.DEFAULT_TABLE);
            s.executeUpdate("DROP TABLE IF EXISTS " + JdbcSnapshotProgressStore.PROGRESS_TABLE);
            s.executeUpdate("DROP TABLE IF EXISTS " + JdbcSnapshotProgressStore.META_TABLE);
            s.executeUpdate("DROP TABLE IF EXISTS " + JdbcSnapshotProgressStore.CHUNKS_TABLE);
            s.executeUpdate("CREATE TABLE orders ("
                    + "id BIGINT PRIMARY KEY, amount BIGINT, status VARCHAR(64))");
        }
    }

    private static NavigableMap<Object, Record> readSource(DataSource ds, TableRef ref)
            throws SQLException {
        NavigableMap<Object, Record> map = new TreeMap<>(ref.order());
        try (Connection c = ds.getConnection();
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
