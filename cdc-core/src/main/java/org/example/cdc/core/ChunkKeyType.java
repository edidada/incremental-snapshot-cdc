package org.example.cdc.core;

/**
 * Physical type of the split (primary) key. Connectors use it to decide how to
 * encode/decode boundary values and how to sample chunk boundaries.
 */
public enum ChunkKeyType {
    NUMERIC,
    STRING,
    OTHER
}
