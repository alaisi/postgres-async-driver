package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
                return parseHexBinary(new String(value, 2, value.length - 2, StandardCharsets.US_ASCII)); // According to postgres rules bytea should be encoded as ASCII sequence
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> byte[]");
        }
    }

    static byte[] fromBytes(byte[] bytes, Charset encoding) {
        return ("\\x" + printHexBinary(bytes)).getBytes(encoding);
    }

    static String fromBytes(byte[] bytes) {
        return ("\\x" + printHexBinary(bytes));
    }
}
