package org.example.cdc.core;

/** Unchecked failure of the snapshot engine (chunk read / coordination error). */
public class IncrementalSnapshotException extends RuntimeException {

    public IncrementalSnapshotException(String message) {
        super(message);
    }

    public IncrementalSnapshotException(String message, Throwable cause) {
        super(message, cause);
    }
}
