package org.example.cdc.core;

/** Kind of a row-level change captured in the transaction / binlog stream. */
public enum ChangeType {
    INSERT,
    UPDATE,
    DELETE
}
