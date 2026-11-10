package org.example.cdc.core;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Objects;

/**
 * Total ordering over primary-key values.
 *
 * <p>This is deliberately a first-class concept rather than a plain
 * {@code Comparable.compareTo}. A database orders string primary keys using the
 * column <em>collation</em> (for example {@code utf8mb4_general_ci} is
 * case-insensitive), while Java's {@link String#compareTo} is binary. If chunk
 * boundaries are computed with one order and range {@code SELECT}s / window
 * deduplication are evaluated with another, rows fall into the wrong chunks and
 * the snapshot silently produces duplicates or loses rows. The very same
 * comparator instance must therefore be used for splitting, range membership,
 * buffering and materialization.</p>
 */
public final class PrimaryKeyComparator implements Comparator<Object> {

    public enum Mode {
        /** {@code Comparable.compareTo}; binary for strings, natural for numbers. */
        NATURAL,
        /** Byte-wise string comparison ({@code utf8mb4_bin} style). */
        BINARY_STRING,
        /** Case-insensitive comparison ({@code utf8mb4_general_ci} style). */
        CASE_INSENSITIVE,
        /** Numeric comparison via {@link BigDecimal}, independent of the boxed type. */
        NUMERIC
    }

    private final Mode mode;

    private PrimaryKeyComparator(Mode mode) {
        this.mode = Objects.requireNonNull(mode, "mode");
    }

    public static PrimaryKeyComparator natural() {
        return new PrimaryKeyComparator(Mode.NATURAL);
    }

    public static PrimaryKeyComparator binaryString() {
        return new PrimaryKeyComparator(Mode.BINARY_STRING);
    }

    public static PrimaryKeyComparator caseInsensitive() {
        return new PrimaryKeyComparator(Mode.CASE_INSENSITIVE);
    }

    public static PrimaryKeyComparator numeric() {
        return new PrimaryKeyComparator(Mode.NUMERIC);
    }

    public static PrimaryKeyComparator of(Mode mode) {
        return new PrimaryKeyComparator(mode);
    }

    public Mode mode() {
        return mode;
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        switch (mode) {
            case NUMERIC:
                return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString()));
            case BINARY_STRING:
                return a.toString().compareTo(b.toString());
            case CASE_INSENSITIVE:
                if (a instanceof CharSequence && b instanceof CharSequence) {
                    int ci = a.toString().compareToIgnoreCase(b.toString());
                    // Binary tie-break keeps the order total (a strict weak ordering
                    // would otherwise collapse distinct keys that only differ by case).
                    return ci != 0 ? ci : a.toString().compareTo(b.toString());
                }
                return compareNatural(a, b);
            case NATURAL:
            default:
                return compareNatural(a, b);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareNatural(Object a, Object b) {
        if (a.getClass() == b.getClass() && a instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }
        if (a instanceof Number && b instanceof Number) {
            return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString()));
        }
        return a.toString().compareTo(b.toString());
    }

    @Override
    public String toString() {
        return "PrimaryKeyComparator(" + mode + ")";
    }
}
