package com.github.pgasync.conversion;

import com.pgasync.SqlException;
import com.github.pgasync.Oid;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author Antti Laisi
 */
class NumericConversions {

    static Long toLong(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8:
                return Long.valueOf(value);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Long");
        }
    }

    static Integer toInteger(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4:
                return Integer.valueOf(value);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Integer");
        }
    }

    static Short toShort(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2:
                return Short.valueOf(value);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Short");
        }
    }

    static Byte toByte(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2:
                return Byte.valueOf(value);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Byte");
        }
    }

    static BigInteger toBigInteger(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8:
                return new BigInteger(value);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> BigInteger");
        }
    }

    static BigDecimal toBigDecimal(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8: // fallthrough
            case NUMERIC: // fallthrough
            case FLOAT4: // fallthrough
            case FLOAT8:
                return new BigDecimal(value);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> BigDecimal");
        }
    }

    static Double toDouble(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8: // fallthrough
            case NUMERIC: // fallthrough
            case FLOAT4: // fallthrough
            case FLOAT8:
                return Double.valueOf(value);
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Double");
        }
    }

}
