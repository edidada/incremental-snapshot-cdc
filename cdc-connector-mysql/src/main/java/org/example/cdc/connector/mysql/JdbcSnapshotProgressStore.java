package org.example.cdc.connector.mysql;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.ChunkPlan;
import org.example.cdc.core.IncrementalSnapshotException;
import org.example.cdc.core.SnapshotCheckpoint;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.spi.SnapshotProgressStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Durable checkpoint store backed by three small JDBC tables. */
public final class JdbcSnapshotProgressStore implements SnapshotProgressStore {

    public static final String PROGRESS_TABLE = "_cdc_snapshot_progress";
    public static final String META_TABLE = "_cdc_snapshot_meta";
    public static final String CHUNKS_TABLE = "_cdc_snapshot_chunks";

    private final DataSource dataSource;
    private final TableRef table;

    public JdbcSnapshotProgressStore(DataSource dataSource, TableRef table) {
        this.dataSource = dataSource;
        this.table = table;
    }

    public void initializeSchema() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + PROGRESS_TABLE + " ("
                    + "table_name VARCHAR(255) NOT NULL, "
                    + "chunk_id VARCHAR(255) NOT NULL, "
                    + "PRIMARY KEY (table_name, chunk_id))");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + META_TABLE + " ("
                    + "table_name VARCHAR(255) NOT NULL PRIMARY KEY, "
                    + "snapshot_start BIGINT NOT NULL, "
                    + "updated_at BIGINT NOT NULL)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + CHUNKS_TABLE + " ("
                    + "table_name VARCHAR(255) NOT NULL, "
                    + "idx INT NOT NULL, "
                    + "chunk_id VARCHAR(255) NOT NULL, "
                    + "lower_value VARCHAR(255), "
                    + "upper_value VARCHAR(255), "
                    + "include_upper BOOLEAN NOT NULL, "
                    + "total INT NOT NULL, "
                    + "snapshot_start BIGINT NOT NULL, "
                    + "PRIMARY KEY (table_name, idx))");
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to create progress schema", e);
        }
    }

    @Override
    public synchronized Optional<SnapshotCheckpoint> loadCheckpoint(String tableName) {
        try (Connection connection = dataSource.getConnection()) {
            Long start = null;
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT snapshot_start FROM " + META_TABLE + " WHERE table_name = ?")) {
                ps.setString(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        start = rs.getLong(1);
                    }
                }
            }
            if (start == null) {
                return Optional.empty();
            }
            Set<String> completed = new LinkedHashSet<>();
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT chunk_id FROM " + PROGRESS_TABLE + " WHERE table_name = ?")) {
                ps.setString(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        completed.add(rs.getString(1));
                    }
                }
            }
            return Optional.of(new SnapshotCheckpoint(tableName, start, completed,
                    System.currentTimeMillis()));
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to load checkpoint for " + tableName, e);
        }
    }

    @Override
    public synchronized void savePlan(String tableName, long snapshotStart, List<Chunk> plan) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement ps = connection.prepareStatement(
                        "DELETE FROM " + CHUNKS_TABLE + " WHERE table_name = ?")) {
                    ps.setString(1, tableName);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO " + CHUNKS_TABLE
                                + " (table_name, idx, chunk_id, lower_value, upper_value,"
                                + " include_upper, total, snapshot_start) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                    for (Chunk chunk : plan) {
                        ps.setString(1, tableName);
                        ps.setInt(2, chunk.index());
                        ps.setString(3, chunk.id());
                        ps.setString(4, KeyCodec.stringify(chunk.lower()));
                        ps.setString(5, KeyCodec.stringify(chunk.upper()));
                        ps.setBoolean(6, chunk.includeUpper());
                        ps.setInt(7, chunk.total());
                        ps.setLong(8, snapshotStart);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to save chunk plan for " + tableName, e);
        }
    }

    @Override
    public synchronized Optional<ChunkPlan> loadPlan(String tableName) {
        String sql = "SELECT idx, chunk_id, lower_value, upper_value, include_upper,"
                + " total, snapshot_start FROM " + CHUNKS_TABLE
                + " WHERE table_name = ? ORDER BY idx";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableName);
            List<Chunk> chunks = new ArrayList<>();
            long snapshotStart = 0L;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    snapshotStart = rs.getLong(7);
                    Object lower = KeyCodec.parse(rs.getString(3), table.keyType());
                    Object upper = KeyCodec.parse(rs.getString(4), table.keyType());
                    chunks.add(new Chunk(rs.getString(2), tableName, table.primaryKeyColumn(),
                            lower, upper, rs.getBoolean(5), rs.getInt(1), rs.getInt(6),
                            table.order()));
                }
            }
            return chunks.isEmpty()
                    ? Optional.empty()
                    : Optional.of(new ChunkPlan(tableName, snapshotStart, chunks));
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to load chunk plan for " + tableName, e);
        }
    }

    @Override
    public synchronized void markCompleted(String tableName, String chunkId, long snapshotStart) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement ps = connection.prepareStatement(
                        "DELETE FROM " + PROGRESS_TABLE
                                + " WHERE table_name = ? AND chunk_id = ?")) {
                    ps.setString(1, tableName);
                    ps.setString(2, chunkId);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO " + PROGRESS_TABLE + " (table_name, chunk_id) VALUES (?, ?)")) {
                    ps.setString(1, tableName);
                    ps.setString(2, chunkId);
                    ps.executeUpdate();
                }
                // snapshotStart is constant for the whole run, so a delete+insert is
                // portable (no vendor-specific UPSERT needed).
                try (PreparedStatement ps = connection.prepareStatement(
                        "DELETE FROM " + META_TABLE + " WHERE table_name = ?")) {
                    ps.setString(1, tableName);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO " + META_TABLE
                                + " (table_name, snapshot_start, updated_at) VALUES (?, ?, ?)")) {
                    ps.setString(1, tableName);
                    ps.setLong(2, snapshotStart);
                    ps.setLong(3, System.currentTimeMillis());
                    ps.executeUpdate();
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to checkpoint " + chunkId, e);
        }
    }

    @Override
    public synchronized void clear(String tableName) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement p1 = connection.prepareStatement(
                     "DELETE FROM " + PROGRESS_TABLE + " WHERE table_name = ?");
             PreparedStatement p2 = connection.prepareStatement(
                     "DELETE FROM " + META_TABLE + " WHERE table_name = ?");
             PreparedStatement p3 = connection.prepareStatement(
                     "DELETE FROM " + CHUNKS_TABLE + " WHERE table_name = ?")) {
            p1.setString(1, tableName);
            p2.setString(1, tableName);
            p3.setString(1, tableName);
            p1.executeUpdate();
            p2.executeUpdate();
            p3.executeUpdate();
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to clear checkpoint for " + tableName, e);
        }
    }
}
