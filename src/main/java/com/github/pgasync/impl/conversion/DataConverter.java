package com.github.pgasync.impl.conversion;

import com.github.pgasync.Converter;
import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Antti Laisi
 */
public class DataConverter {

    final Map<Class<?>, Converter<?>> typeToConverter = new HashMap<>();

    public DataConverter(List<Converter<?>> converters) {
        converters.forEach(c -> typeToConverter.put(c.type(), c));
    }
    public DataConverter() {
        this(Collections.emptyList());
    }

    public String toString(Oid oid, byte[] value) {
        return value == null ? null : StringConversions.toString(oid, value);
    }
    public Character toChar(Oid oid, byte[] value) {
        return value == null ? null : StringConversions.toChar(oid, value);
    }
    public Long toLong(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toLong(oid, value);
    }
    public Integer toInteger(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toInteger(oid, value);
    }
    public Short toShort(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toShort(oid, value);
    }
    public Byte toByte(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toByte(oid, value);
    }
    public BigInteger toBigInteger(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toBigInteger(oid, value);
    }
    public BigDecimal toBigDecimal(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toBigDecimal(oid, value);
    }
    public Date toDate(Oid oid, byte[] value) {
        return value == null ? null : TemporalConversions.toDate(oid, value);
    }
    public Time toTime(Oid oid, byte[] value) {
        return value == null ? null : TemporalConversions.toTime(oid, value);
    }
    public Timestamp toTimestamp(Oid oid, byte[] value) {
        return value == null ? null : TemporalConversions.toTimestamp(oid, value);
    }
    public byte[] toBytes(Oid oid, byte[] value) {
        return value == null ? null : BlobConversions.toBytes(oid, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T toObject(Class<T> type, Oid oid, byte[] value) {
        Converter converter = typeToConverter.get(type);
        if(converter == null) {
            throw new IllegalArgumentException("Unknown conversion target: " + value.getClass());
        }
        return (T) converter.to(oid, value);
    }

    public byte[] fromObject(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Time) {
            return TemporalConversions.fromTime((Time) o);
        }
        if (o instanceof Date) {
            return TemporalConversions.fromDate((Date) o);
        }
        if (o instanceof byte[]) {
            return BlobConversions.fromBytes((byte[]) o);
        }
        if(o instanceof String || o instanceof Number || o instanceof Character || o instanceof UUID) {
            return o.toString().getBytes(UTF_8);
        }
        return fromConvertable(o);
    }

    @SuppressWarnings("unchecked")
    protected byte[] fromConvertable(Object value) {
        Converter converter = typeToConverter.get(value.getClass());
        if(converter == null) {
            throw new IllegalArgumentException("Unknown conversion target: " + value.getClass());
        }
        return converter.from(value);
    }

    public byte[][] fromParameters(List<Object> parameters) {
        byte[][] params = new byte[parameters.size()][];
        int i = 0;
        for (Object param : parameters) {
            params[i++] = fromObject(param);
        }
        return params;
    }

    public Object toObject(Oid oid, byte[] value) {
        if(value == null) {
            return null;
        }
        switch (oid) {
            case TEXT: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR: // fallthrough
            case VARCHAR: return toString(oid, value);
            case INT2: return toShort(oid, value);
            case INT4: return toInteger(oid, value);
            case INT8: return toLong(oid, value);
            case FLOAT4: // fallthrough
            case FLOAT8: return toBigDecimal(oid, value);
            case BYTEA: return toBytes(oid, value);
            default:
                return toComplexObject(oid, value);
        }
    }

    protected Object toComplexObject(Oid oid, byte[] value) {
        switch (oid) {
            case DATE: return toDate(oid, value);
            case TIMETZ: // fallthrough
            case TIME: return toTime(oid, value);
            case TIMESTAMP: // fallthrough
            case TIMESTAMPTZ: return toTimestamp(oid, value);
            default:
                return toComplexObject(oid, value);
        }
    }
}
