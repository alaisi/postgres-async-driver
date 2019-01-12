package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author Antti Laisi
 */
class StringConversions {

    static String toString(Oid oid, byte[] value, Charset charset) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case TEXT: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR: // fallthrough
            case UUID: // fallthrough
            case VARCHAR:
                return new String(value, charset);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> String");
        }
    }

    static Character toChar(Oid oid, byte[] value, Charset charset) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR:
                return charset.decode(ByteBuffer.wrap(value)).charAt(0);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> String");
        }
    }
}
