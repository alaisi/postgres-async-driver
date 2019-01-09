package com.github.pgasync.impl.conversion;

import com.github.pgasync.Converter;
import com.github.pgasync.impl.Oid;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Antti Laisi
 */
public class DataConverter {

    private final Map<Class<?>, Converter<?>> typeToConverter;

    public DataConverter(List<Converter<?>> converters) {
        typeToConverter = converters.stream()
                .collect(Collectors.toMap(Converter::type, Function.identity()));
    }

    public DataConverter() {
        this(List.of());
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

    public Double toDouble(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toDouble(oid, value);
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

    public Boolean toBoolean(Oid oid, byte[] value) {
        return value == null ? null : BooleanConversions.toBoolean(oid, value);
    }

    public <TArray> TArray toArray(Class<TArray> arrayType, Oid oid, byte[] value) {
        switch (oid) {
            case INT2_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, NumericConversions::toShort);
            case INT4_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, NumericConversions::toInteger);
            case INT8_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, NumericConversions::toLong);

            case TEXT_ARRAY:
            case CHAR_ARRAY:
            case BPCHAR_ARRAY:
            case VARCHAR_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, StringConversions::toString);

            case NUMERIC_ARRAY:
            case FLOAT4_ARRAY:
            case FLOAT8_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, NumericConversions::toBigDecimal);

            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, TemporalConversions::toTimestamp);

            case TIMETZ_ARRAY:
            case TIME_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, TemporalConversions::toTime);

            case DATE_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, TemporalConversions::toDate);

            case BOOL_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, value, BooleanConversions::toBoolean);
            default:
                throw new IllegalStateException("Unsupported array type: " + oid);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T toObject(Class<T> type, Oid oid, byte[] value) {
        Converter converter = typeToConverter.get(type);
        if (converter == null) {
            throw new IllegalArgumentException("Unknown conversion target: " + value.getClass());
        }
        return (T) converter.to(oid, value);
    }

    private byte[] fromObject(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Time) {
            return TemporalConversions.fromTime((Time) o);
        }
        if (o instanceof Timestamp) {
            return TemporalConversions.fromTimestamp((Timestamp) o);
        }
        if (o instanceof Date) {
            return TemporalConversions.fromDate((Date) o);
        }
        if (o instanceof byte[]) {
            return BlobConversions.fromBytes((byte[]) o);
        }
        if (o instanceof Boolean) {
            return BooleanConversions.fromBoolean((boolean) o);
        }
        if (o.getClass().isArray()) {
            return ArrayConversions.fromArray(o, this::fromObject);
        }
        if (o instanceof String || o instanceof Number || o instanceof Character || o instanceof UUID) {
            return o.toString().getBytes(UTF_8);
        }
        return fromConvertable(o);
    }

    @SuppressWarnings("unchecked")
    private byte[] fromConvertable(Object value) {
        Converter converter = typeToConverter.get(value.getClass());
        if (converter == null) {
            throw new IllegalArgumentException("Unknown conversion target: " + value.getClass());
        }
        return converter.from(value);
    }

    public byte[][] fromParameters(List<Object> parameters) {
        return fromParameters(parameters.toArray(new Object[]{}));
    }

    public byte[][] fromParameters(Object[] parameters) {
        byte[][] params = new byte[parameters.length][];
        int i = 0;
        for (Object param : parameters) {
            params[i++] = fromObject(param);
        }
        return params;
    }

    public Object toObject(Oid oid, byte[] value) {
        if (value == null) {
            return null;
        }
        switch (oid) {
            case TEXT: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR: // fallthrough
            case VARCHAR:
                return toString(oid, value);
            case INT2:
                return toShort(oid, value);
            case INT4:
                return toInteger(oid, value);
            case INT8:
                return toLong(oid, value);
            case NUMERIC: // fallthrough
            case FLOAT4: // fallthrough
            case FLOAT8:
                return toBigDecimal(oid, value);
            case BYTEA:
                return toBytes(oid, value);
            case DATE:
                return toDate(oid, value);
            case TIMETZ: // fallthrough
            case TIME:
                return toTime(oid, value);
            case TIMESTAMP: // fallthrough
            case TIMESTAMPTZ:
                return toTimestamp(oid, value);
            case UUID:
                return UUID.fromString(toString(oid, value));
            case BOOL:
                return toBoolean(oid, value);

            case INT2_ARRAY:
            case INT4_ARRAY:
            case INT8_ARRAY:
            case NUMERIC_ARRAY:
            case FLOAT4_ARRAY:
            case FLOAT8_ARRAY:
            case TEXT_ARRAY:
            case CHAR_ARRAY:
            case BPCHAR_ARRAY:
            case VARCHAR_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY:
            case TIMETZ_ARRAY:
            case TIME_ARRAY:
            case BOOL_ARRAY:
                return toArray(Object[].class, oid, value);
            default:
                return toConvertable(oid, value);
        }
    }

    private Object toConvertable(Oid oid, byte[] value) {
        throw new IllegalStateException("Unknown conversion source: " + oid);
    }

}
