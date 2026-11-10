package org.example.cdc.core;

import java.util.Locale;

/** Canonical, deterministic chunk id (stable across restarts for recovery). */
public final class ChunkIds {

    private ChunkIds() {
    }

    public static String format(String table, int index) {
        return table + "#" + String.format(Locale.ROOT, "%05d", index);
    }
}
