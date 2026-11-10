package org.example.cdc.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimaryKeyComparatorTest {

    @Test
    void numericOrderIsIndependentOfBoxedType() {
        PrimaryKeyComparator cmp = PrimaryKeyComparator.numeric();
        assertEquals(0, cmp.compare(7, 7L));
        assertTrue(cmp.compare(2L, 10) < 0);
        assertTrue(cmp.compare(new java.math.BigDecimal("3.5"), 3) > 0);
    }

    @Test
    void binaryStringUsesByteOrder() {
        PrimaryKeyComparator cmp = PrimaryKeyComparator.binaryString();
        assertTrue(cmp.compare("Z", "a") < 0);   // 'Z'(90) < 'a'(97)
        assertTrue(cmp.compare("A", "a") < 0);
        assertTrue(cmp.compare("a", "A") > 0);
    }

    @Test
    void caseInsensitiveMatchesCollationButStaysTotal() {
        PrimaryKeyComparator cmp = PrimaryKeyComparator.caseInsensitive();
        assertTrue(cmp.compare("apple", "Banana") < 0);  // a < b ignoring case
        assertTrue(cmp.compare("Zebra", "apple") > 0);
        // Binary tie-break keeps 'A' and 'a' distinct-but-ordered.
        assertTrue(cmp.compare("A", "a") < 0);
        assertEquals(0, cmp.compare("APPLE", "APPLE"));
    }

    @Test
    void comparesNullsDeterministically() {
        PrimaryKeyComparator cmp = PrimaryKeyComparator.natural();
        assertTrue(cmp.compare(null, 1L) < 0);
        assertTrue(cmp.compare(1L, null) > 0);
        assertEquals(0, cmp.compare(null, null));
    }
}
