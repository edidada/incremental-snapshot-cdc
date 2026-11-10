package org.example.cdc.connector.mysql;

import org.example.cdc.core.ChunkKeyType;

import java.math.BigDecimal;
import java.math.BigInteger;

/** Normalizes primary-key values to a canonical Java type per {@link ChunkKeyType}. */
public final class KeyCodec {

    private KeyCodec() {
    }

    public static Object canonicalize(Object raw, ChunkKeyType type) {
        if (raw == null) {
            return null;
        }
        switch (type) {
            case NUMERIC:
                if (raw instanceof Byte || raw instanceof Short
                        || raw instanceof Integer || raw instanceof Long
                        || raw instanceof BigInteger) {
                    return ((Number) raw).longValue();
                }
                return new BigDecimal(raw.toString());
            case STRING:
                return raw.toString();
            default:
                return raw;
        }
    }

    /** Inverse of the string form stored in the changelog / progress tables. */
    public static Object parse(String stored, ChunkKeyType type) {
        if (stored == null) {
            return null;
        }
        switch (type) {
            case NUMERIC:
                try {
                    return Long.parseLong(stored);
                } catch (NumberFormatException e) {
                    return new BigDecimal(stored);
                }
            case STRING:
            default:
                return stored;
        }
    }

    public static String stringify(Object canonical) {
        return canonical == null ? null : canonical.toString();
    }
}
