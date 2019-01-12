package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;

/**
 * @author Antti Laisi
 */
class NumericConversions {

    static Long toLong(Oid oid, byte[] value, Charset encoding) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8:
                return Long.valueOf(new String(value, encoding));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Long");
        }
    }

    static Integer toInteger(Oid oid, byte[] value, Charset encoding) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4:
                return Integer.valueOf(new String(value, encoding));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Integer");
        }
    }

    static Short toShort(Oid oid, byte[] value, Charset encoding) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2:
                return Short.valueOf(new String(value, encoding));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Short");
        }
    }

    static Byte toByte(Oid oid, byte[] value, Charset encoding) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2:
                return Byte.valueOf(new String(value, encoding));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Byte");
        }
    }

    static BigInteger toBigInteger(Oid oid, byte[] value, Charset encoding) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8:
                return new BigInteger(new String(value, encoding));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> BigInteger");
        }
    }

    static BigDecimal toBigDecimal(Oid oid, byte[] value, Charset encoding) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8: // fallthrough
            case NUMERIC: // fallthrough
            case FLOAT4: // fallthrough
            case FLOAT8:
                return new BigDecimal(new String(value, encoding));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> BigDecimal");
        }
    }

    static Double toDouble(Oid oid, byte[] value, Charset encoding) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8: // fallthrough
            case NUMERIC: // fallthrough
            case FLOAT4: // fallthrough
            case FLOAT8:
                return Double.valueOf(new String(value, encoding));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Double");
        }
    }

}
