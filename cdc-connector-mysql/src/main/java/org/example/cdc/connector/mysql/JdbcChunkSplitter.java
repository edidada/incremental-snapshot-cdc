package org.example.cdc.connector.mysql;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.ChunkIds;
import org.example.cdc.core.ChunkKeyType;
import org.example.cdc.core.IncrementalSnapshotException;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.spi.ChunkSplitter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Sampling-based chunk splitter. It reads the primary-key boundaries with
 * {@code ORDER BY pk LIMIT 1 OFFSET n} so chunks are roughly even regardless of
 * key type (numeric or string). This is the strategy Flink CDC uses for
 * non-uniform / string keys; it avoids arithmetic splitting that assumes a
 * numeric, evenly distributed key.
 */
public final class JdbcChunkSplitter implements ChunkSplitter {

    private final DataSource dataSource;

    public JdbcChunkSplitter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<Chunk> split(TableRef table, int chunkSize) {
        String t = SqlIdentifiers.require(table.table());
        String pk = SqlIdentifiers.require(table.primaryKeyColumn());
        try (Connection connection = dataSource.getConnection()) {
            long count = countRows(connection, t);
            if (count == 0) {
                return List.of(new Chunk(ChunkIds.format(t, 0), t, pk,
                        null, null, true, 0, 1, table.order()));
            }
            int total = (int) ((count + chunkSize - 1) / chunkSize);
            List<Chunk> chunks = new ArrayList<>(total);
            for (long start = 0, index = 0; start < count; start += chunkSize, index++) {
                Object lower = start == 0
                        ? null
                        : boundaryAt(connection, t, pk, (int) start, table.keyType());
                long end = Math.min(start + chunkSize, count);
                Object upper = end == count
                        ? null
                        : boundaryAt(connection, t, pk, (int) end, table.keyType());
                chunks.add(new Chunk(ChunkIds.format(t, (int) index), t, pk,
                        lower, upper, false, (int) index, total, table.order()));
            }
            return chunks;
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("failed to split " + t, e);
        }
    }

    private long countRows(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private Object boundaryAt(Connection connection, String table, String pk,
                              int offset, ChunkKeyType keyType)
            throws SQLException {
        String sql = "SELECT " + pk + " FROM " + table
                + " ORDER BY " + pk + " LIMIT 1 OFFSET ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, offset);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("no boundary key at offset " + offset);
                }
                return KeyCodec.canonicalize(rs.getObject(1), keyType);
            }
        }
    }
}
