package org.example.cdc.connector.mysql;

import org.example.cdc.core.ChangeEvent;
import org.example.cdc.core.ChangeType;
import org.example.cdc.core.IncrementalSnapshotException;
import org.example.cdc.core.Record;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.spi.ChangeLog;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link ChangeLog} backed by a changelog table ({@code _cdc_binlog}). This is a
 * common production pattern: DML is captured (by triggers, audit tables, or a
 * binlog-to-table shipper) into an ordered log table with a monotonic position.
 *
 * <p>A direct {@code mysql-binlog-connector-java} stream can be adapted to the
 * same {@link ChangeLog} SPI without touching the engine; the table adapter is
 * used here because it is fully testable on an embedded JDBC database.</p>
 */
public final class JdbcChangeLog implements ChangeLog {

    public static final String DEFAULT_TABLE = "_cdc_binlog";

    private final DataSource dataSource;
    private final TableRef table;
    private final String logTable;
    private final RowCodec codec;

    public JdbcChangeLog(DataSource dataSource, TableRef table) {
        this(dataSource, table, DEFAULT_TABLE, RowCodec.jdkSerialization());
    }

    public JdbcChangeLog(DataSource dataSource, TableRef table,
                         String logTable, RowCodec codec) {
        this.dataSource = dataSource;
        this.table = table;
        this.logTable = logTable;
        this.codec = codec;
    }

    public void initializeSchema() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + logTable + " ("
                    + "pos BIGINT PRIMARY KEY, "
                    + "table_name VARCHAR(255) NOT NULL, "
                    + "op VARCHAR(16) NOT NULL, "
                    + "pk_value VARCHAR(255), "
                    + "after_payload VARCHAR(8192))");
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to create changelog table", e);
        }
    }

    /** Append a change and return its assigned position. */
    public synchronized long append(String tableName, ChangeType type,
                                    Object primaryKey, Record after) {
        if (type != ChangeType.DELETE && after == null) {
            throw new IllegalArgumentException("after image is required for " + type);
        }
        long position = maxPosition() + 1;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "INSERT INTO " + logTable
                             + " (pos, table_name, op, pk_value, after_payload) VALUES (?, ?, ?, ?, ?)")) {
            ps.setLong(1, position);
            ps.setString(2, tableName);
            ps.setString(3, type.name());
            ps.setString(4, KeyCodec.stringify(primaryKey));
            ps.setString(5, after == null ? null : codec.encode(after));
            ps.executeUpdate();
            return position;
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to append change", e);
        }
    }

    @Override
    public synchronized long currentPosition() {
        return maxPosition();
    }

    private long maxPosition() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT COALESCE(MAX(pos), 0) FROM " + logTable)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to read changelog position", e);
        }
    }

    @Override
    public synchronized List<ChangeEvent> readRange(String tableName,
                                                    long lowExclusive, long highInclusive) {
        String sql = "SELECT pos, op, pk_value, after_payload FROM " + logTable
                + " WHERE table_name = ? AND pos > ? AND pos <= ? ORDER BY pos";
        List<ChangeEvent> events = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableName);
            ps.setLong(2, lowExclusive);
            ps.setLong(3, highInclusive);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long position = rs.getLong(1);
                    ChangeType type = ChangeType.valueOf(rs.getString(2));
                    Object pk = KeyCodec.parse(rs.getString(3), table.keyType());
                    String payload = rs.getString(4);
                    Record after = payload == null ? null : codec.decode(payload, tableName, pk);
                    events.add(new ChangeEvent(position, type, tableName, pk, after, 0L));
                }
            }
            return events;
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to replay changelog window", e);
        }
    }
}
