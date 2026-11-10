package org.example.cdc.core;

import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkTest {

    private static Chunk chunk(Object lower, Object upper, boolean includeUpper,
                               Comparator<Object> order) {
        return new Chunk("t#00000", "t", "id", lower, upper, includeUpper, 0, 1, order);
    }

    @Test
    void halfOpenRangeIsExclusiveOnUpper() {
        Chunk c = chunk(10L, 20L, false, PrimaryKeyComparator.numeric());
        assertFalse(c.contains(9L));
        assertTrue(c.contains(10L));
        assertTrue(c.contains(19L));
        assertFalse(c.contains(20L));
    }

    @Test
    void includeUpperMakesLastBoundInclusive() {
        Chunk c = chunk(10L, 20L, true, PrimaryKeyComparator.numeric());
        assertTrue(c.contains(20L));
        assertFalse(c.contains(21L));
    }

    @Test
    void nullBoundsAreUnbounded() {
        Chunk first = chunk(null, 20L, false, PrimaryKeyComparator.numeric());
        assertTrue(first.contains(-5L));
        assertFalse(first.contains(20L));

        Chunk last = chunk(20L, null, true, PrimaryKeyComparator.numeric());
        assertTrue(last.contains(999L));
        assertFalse(last.contains(19L));
    }

    @Test
    void usesCollationAwareOrderForMembership() {
        // Case-insensitive collation: 'Mango' sorts at 'm', so it belongs to
        // a chunk bounded below by 'm' even though binary compare would disagree.
        Chunk c = chunk("m", "z", false, PrimaryKeyComparator.caseInsensitive());
        assertTrue(c.contains("Mango"));
        assertTrue(c.contains("pear"));
        assertFalse(c.contains("apple"));
    }
}
