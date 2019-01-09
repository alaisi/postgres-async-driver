package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.xml.bind.DatatypeConverter.parseHexBinary;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

/**
 * @author Antti Laisi
 */
class BlobConversions {

    static byte[] toBytes(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case BYTEA:
                return parseHexBinary(new String(value, 2, value.length - 2, UTF_8));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> byte[]");
        }
    }

    static byte[] fromBytes(byte[] bytes) {
        return ("\\x" + printHexBinary(bytes)).getBytes(UTF_8);
    }
}
