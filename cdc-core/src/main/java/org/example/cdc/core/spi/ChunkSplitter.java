package org.example.cdc.core.spi;

import org.example.cdc.core.Chunk;
import org.example.cdc.core.TableRef;

import java.util.List;

/** Divides a table into primary-key ranges (chunks) of at most {@code chunkSize} rows. */
public interface ChunkSplitter {

    List<Chunk> split(TableRef table, int chunkSize);
}
