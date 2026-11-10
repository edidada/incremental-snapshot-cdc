package org.example.cdc.core.impl;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.ChunkIds;
import org.example.cdc.core.TableRef;
import org.example.cdc.core.spi.ChunkSplitter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reference splitter: take the sorted primary keys and cut them into groups of
 * {@code chunkSize}. Boundaries are {@code [lower, upper)} (first/last chunk are
 * unbounded on the outside), so contiguous groups form an exact partition.
 *
 * <p>The order used to <em>choose</em> boundary keys is {@code splitOrder}. By
 * default it is the table's collation, which is the only correct choice. A
 * different order can be injected to reproduce the classic
 * {@code utf8mb4_general_ci} bug where Java's binary {@code String.compareTo}
 * disagrees with the database collation (see {@code CollationBoundaryIT}).</p>
 */
public final class RangeChunkSplitter implements ChunkSplitter {

    private final SimulatedDatabase db;
    private final Comparator<Object> splitOrder; // null => use table collation

    public RangeChunkSplitter(SimulatedDatabase db) {
        this(db, null);
    }

    public RangeChunkSplitter(SimulatedDatabase db, Comparator<Object> splitOrder) {
        this.db = db;
        this.splitOrder = splitOrder;
    }

    @Override
    public List<Chunk> split(TableRef table, int chunkSize) {
        Comparator<Object> choose = splitOrder != null ? splitOrder : table.order();
        List<Object> keys = new ArrayList<>(db.sortedPrimaryKeys());
        keys.sort(choose);

        if (keys.isEmpty()) {
            return List.of(unbounded(table, 0, 1, table.order()));
        }

        int n = keys.size();
        int total = (n + chunkSize - 1) / chunkSize;
        List<Chunk> chunks = new ArrayList<>(total);
        for (int start = 0, index = 0; start < n; start += chunkSize, index++) {
            int end = Math.min(start + chunkSize, n);
            Object lower = start == 0 ? null : keys.get(start);
            Object upper = end == n ? null : keys.get(end); // exclusive
            chunks.add(new Chunk(ChunkIds.format(table.table(), index), table.table(),
                    table.primaryKeyColumn(), lower, upper, false,
                    index, total, table.order()));
        }
        return chunks;
    }

    /** Kept for backwards-compatible use in tests; delegates to {@link ChunkIds}. */
    public static String chunkId(String table, int index) {
        return ChunkIds.format(table, index);
    }

    private static Chunk unbounded(TableRef table, int index, int total,
                                   Comparator<Object> order) {
        return new Chunk(ChunkIds.format(table.table(), index), table.table(),
                table.primaryKeyColumn(), null, null, true, index, total, order);
    }
}
