package com.github.pgasync.conversion;

import com.pgasync.SqlException;
import com.github.pgasync.Oid;

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
