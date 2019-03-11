package com.github.pgasync.conversion;

import com.pgasync.SqlException;
import com.github.pgasync.Oid;

/**
 * @author Antti Laisi
 */
class StringConversions {

    static String toString(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case TEXT: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR: // fallthrough
            case UUID: // fallthrough
            case VARCHAR:
                return value;
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> String");
        }
    }

    static Character toChar(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR:
                return value.charAt(0);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Char");
        }
    }
}
