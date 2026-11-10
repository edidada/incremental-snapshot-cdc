package org.example.cdc.connector.mysql;

import org.example.cdc.core.ChangeType;
import org.example.cdc.core.IncrementalSnapshotException;
import org.example.cdc.core.Record;
import org.example.cdc.core.SnapshotConfig;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.engine.SnapshotCoordinator;
import org.example.cdc.core.spi.RecordSink;
import org.example.cdc.core.spi.SnapshotListener;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Convenience wiring for a MySQL/JDBC source: creates the engine-owned schema,
 * exposes the four SPI implementations, and offers a {@link #apply} helper that
 * performs DML on the source table while recording the change in the changelog
 * (the application-side counterpart of a real binlog).
 *
 * <p>In production the source DML would be performed by the application and the
 * changelog filled by a binlog shipper/triggers; {@code apply} just makes the
 * whole system runnable and testable against a real JDBC database.</p>
 */
public final class JdbcSnapshotEnvironment {

    private final DataSource dataSource;
    private final TableRef table;
    private final JdbcChunkSplitter splitter;
    private final JdbcSnapshotReader reader;
    private final JdbcSnapshotProgressStore progressStore;
    private final JdbcChangeLog changeLog;

    public JdbcSnapshotEnvironment(DataSource dataSource, TableRef table) {
        SqlIdentifiers.require(table.table());
        SqlIdentifiers.require(table.primaryKeyColumn());
        this.dataSource = dataSource;
        this.table = table;
        this.splitter = new JdbcChunkSplitter(dataSource);
        this.reader = new JdbcSnapshotReader(dataSource, table);
        this.progressStore = new JdbcSnapshotProgressStore(dataSource, table);
        this.changeLog = new JdbcChangeLog(dataSource, table);
    }

    /** Create the progress and changelog tables (the source table is caller-owned). */
    public void initializeSchema() {
        progressStore.initializeSchema();
        changeLog.initializeSchema();
    }

    public JdbcChunkSplitter splitter() {
        return splitter;
    }

    public JdbcSnapshotReader reader() {
        return reader;
    }

    public JdbcSnapshotProgressStore progressStore() {
        return progressStore;
    }

    public JdbcChangeLog changeLog() {
        return changeLog;
    }

    public SnapshotCoordinator coordinator(SnapshotConfig config, RecordSink sink,
                                           SnapshotListener listener) {
        return new SnapshotCoordinator(table, splitter, reader, changeLog,
                progressStore, sink, config, listener);
    }

    /** Apply DML to the source table and append the corresponding changelog event. */
    public synchronized long apply(ChangeType type, Object primaryKey,
                                   Map<String, Object> values) {
        String t = table.table();
        String pkColumn = table.primaryKeyColumn();
        Object key = KeyCodec.canonicalize(primaryKey, table.keyType());
        Map<String, Object> row = new LinkedHashMap<>(values);
        row.put(pkColumn, key);

        try (Connection connection = dataSource.getConnection()) {
            switch (type) {
                case INSERT:
                    insertRow(connection, t, row);
                    break;
                case UPDATE:
                    updateRow(connection, t, pkColumn, key, row);
                    break;
                case DELETE:
                    deleteRow(connection, t, pkColumn, key);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported change type " + type);
            }
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to apply " + type + " on " + t, e);
        }

        Record after = type == ChangeType.DELETE ? null : new Record(t, key, row);
        return changeLog.append(t, type, key, after);
    }

    private static void insertRow(Connection connection, String table,
                                  Map<String, Object> row) throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" (");
        StringBuilder placeholders = new StringBuilder();
        boolean first = true;
        for (String column : row.keySet()) {
            SqlIdentifiers.require(column);
            if (!first) {
                sql.append(", ");
                placeholders.append(", ");
            }
            sql.append(column);
            placeholders.append('?');
            first = false;
        }
        sql.append(") VALUES (").append(placeholders).append(')');
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int i = 1;
            for (Object value : row.values()) {
                ps.setObject(i++, value);
            }
            ps.executeUpdate();
        }
    }

    private static void updateRow(Connection connection, String table, String pkColumn,
                                  Object key, Map<String, Object> row) throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE ").append(table).append(" SET ");
        boolean first = true;
        for (String column : row.keySet()) {
            SqlIdentifiers.require(column);
            if (column.equalsIgnoreCase(pkColumn)) {
                continue;
            }
            if (!first) {
                sql.append(", ");
            }
            sql.append(column).append(" = ?");
            first = false;
        }
        if (first) {
            throw new IllegalArgumentException("UPDATE requires at least one non-key column");
        }
        sql.append(" WHERE ").append(pkColumn).append(" = ?");
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int i = 1;
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (!entry.getKey().equalsIgnoreCase(pkColumn)) {
                    ps.setObject(i++, entry.getValue());
                }
            }
            ps.setObject(i, key);
            ps.executeUpdate();
        }
    }

    private static void deleteRow(Connection connection, String table,
                                  String pkColumn, Object key) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM " + table + " WHERE " + pkColumn + " = ?")) {
            ps.setObject(1, key);
            ps.executeUpdate();
        }
    }
}
