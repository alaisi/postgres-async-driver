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

    static byte[] toBytes(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case BYTEA:
                return parseHexBinary(value); // TODO: Add theses considerations somewhere to the code: 1. (2, length-2) 2. According to postgres rules bytea should be encoded as ASCII sequence
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> byte[]");
        }
    }

    static String fromBytes(byte[] bytes) {
        return ("\\x" + printHexBinary(bytes));
    }
}
