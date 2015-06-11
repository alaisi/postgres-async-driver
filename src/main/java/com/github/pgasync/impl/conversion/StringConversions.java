package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Antti Laisi
 */
enum StringConversions {
    ;

    static String toString(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case TEXT: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR: // fallthrough
            case UUID: // fallthrough
            case VARCHAR:
                return new String(value, UTF_8);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> String");
        }
    }

    static Character toChar(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR:
                return UTF_8.decode(ByteBuffer.wrap(value)).charAt(0);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> String");
        }
    }
}
