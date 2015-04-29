package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

/**
 * @author Antti Laisi
 */
public class BooleanConversions {
    ;

    static final byte[] TRUE  = new byte[]{ 't' };
    static final byte[] FALSE = new byte[]{ 'f' };

    static boolean toBoolean(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case BOOL:
                return TRUE[0] == value[0];
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> boolean");
        }
    }

    static byte[] fromBoolean(boolean value) {
        return value ? TRUE : FALSE;
    }
}
