package org.example.cdc.connector.mysql;

import java.util.regex.Pattern;

/**
 * Guards dynamic SQL. Table and column names come from configuration, never from
 * end-user input, but validating them anyway prevents identifier injection.
 */
public final class SqlIdentifiers {

    private static final Pattern VALID = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private SqlIdentifiers() {
    }

    public static String require(String identifier) {
        if (identifier == null || !VALID.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + identifier);
        }
        return identifier;
    }
}
