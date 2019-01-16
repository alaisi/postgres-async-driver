package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

/**
 * @author Antti Laisi
 */
class BooleanConversions {

    private static final String TRUE = "t";
    private static final String FALSE = "f";

    static boolean toBoolean(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case BOOL:
                return TRUE.equals(value);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> boolean");
        }
    }

    static String fromBoolean(boolean value) {
        return value ? TRUE : FALSE;
    }
}
