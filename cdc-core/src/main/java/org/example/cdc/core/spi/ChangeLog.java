package org.example.cdc.core.spi;

import org.example.cdc.core.ChangeEvent;

import java.util.List;

/**
 * Positioned, append-only change stream (binlog / WAL abstraction).
 *
 * <p>Positions are monotonic {@code long}s. A chunk window is exactly the
 * half-open interval {@code (LOW, HIGH]}; {@link #currentPosition()} supplies
 * both watermarks and {@link #readRange} replays the window. In a parallel
 * engine this is also the single authoritative stream used to stitch all chunk
 * windows into one ordered changelog.</p>
 */
public interface ChangeLog {

    /** Current (high) position of the changelog. */
    long currentPosition();

    /**
     * Return changes with {@code lowExclusive < position <= highInclusive} for the
     * given table, ordered by position.
     */
    List<ChangeEvent> readRange(String table, long lowExclusive, long highInclusive);
}
