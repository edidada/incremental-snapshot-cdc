package org.example.cdc.connector.mysql;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.IncrementalSnapshotException;
import org.example.cdc.core.Record;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.spi.SnapshotReader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

/** Executes the chunk range {@code SELECT} against MySQL (or any JDBC source). */
public final class JdbcSnapshotReader implements SnapshotReader {

    private final DataSource dataSource;
    private final TableRef table;
    private volatile Consumer<Chunk> selectHook;

    public JdbcSnapshotReader(DataSource dataSource, TableRef table) {
        this.dataSource = dataSource;
        this.table = table;
    }

    /**
     * Optional instrumentation hook fired inside the SELECT window (after rows
     * are fetched, before they are returned). Used by integration tests to
     * inject deterministic concurrent DML.
     */
    public void setSelectHook(Consumer<Chunk> selectHook) {
        this.selectHook = selectHook;
    }

    @Override
    public List<Record> select(Chunk chunk) {
        String t = SqlIdentifiers.require(table.table());
        String pk = SqlIdentifiers.require(table.primaryKeyColumn());

        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(t);
        List<Object> params = new ArrayList<>();
        List<String> predicates = new ArrayList<>();
        if (chunk.lower() != null) {
            predicates.add(pk + " >= ?");
            params.add(chunk.lower());
        }
        if (chunk.upper() != null) {
            predicates.add(pk + (chunk.includeUpper() ? " <= ?" : " < ?"));
            params.add(chunk.upper());
        }
        if (!predicates.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", predicates));
        }
        sql.append(" ORDER BY ").append(pk);

        List<Record> rows = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int columns = meta.getColumnCount();
                int pkIndex = findColumnIgnoreCase(meta, pk);
                while (rs.next()) {
                    Map<String, Object> values = new LinkedHashMap<>();
                    for (int i = 1; i <= columns; i++) {
                        // Normalize labels so a baseline row and a changelog-decoded
                        // row compare equal regardless of the DB's identifier casing.
                        values.put(meta.getColumnLabel(i).toLowerCase(Locale.ROOT),
                                rs.getObject(i));
                    }
                    Object key = KeyCodec.canonicalize(rs.getObject(pkIndex), table.keyType());
                    rows.add(new Record(t, key, values));
                }
            }
        } catch (SQLException e) {
            throw new IncrementalSnapshotException("chunk SELECT failed for " + chunk.id(), e);
        }
        // Fire the hook only after all JDBC resources are released, so injected
        // concurrent DML never contends with an open result set / table lock.
        Consumer<Chunk> hook = selectHook;
        if (hook != null) {
            hook.accept(chunk);
        }
        return rows;
    }

    private static int findColumnIgnoreCase(ResultSetMetaData meta, String name) throws SQLException {
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (meta.getColumnLabel(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        throw new SQLException("primary key column '" + name + "' not found in result set");
    }
}
