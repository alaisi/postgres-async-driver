package com.github.pgasync.impl.conversion;

import com.github.pgasync.Converter;
import com.github.pgasync.impl.Oid;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static javax.xml.bind.DatatypeConverter.parseHexBinary;

/**
 * @author Antti Laisi
 */
public class DataConverter {

    private final Map<Class<?>, Converter<?>> typeToConverter;
    private final Charset encoding;

    public DataConverter(List<Converter<?>> converters, Charset encoding) {
        this.typeToConverter = converters.stream()
                .collect(Collectors.toMap(Converter::type, Function.identity()));
        this.encoding = encoding;
    }

    public DataConverter(Charset encoding) {
        this(List.of(), encoding);
    }

    public String toString(Oid oid, byte[] value) {
        return value == null ? null : StringConversions.toString(oid, new String(value, encoding));
    }

    public Character toChar(Oid oid, byte[] value) {
        return value == null ? null : StringConversions.toChar(oid, new String(value, encoding));
    }

    public Long toLong(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toLong(oid, new String(value, encoding));
    }

    public Integer toInteger(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toInteger(oid, new String(value, encoding));
    }

    public Short toShort(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toShort(oid, new String(value, encoding));
    }

    public Byte toByte(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toByte(oid, new String(value, encoding));
    }

    public BigInteger toBigInteger(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toBigInteger(oid, new String(value, encoding));
    }

    public BigDecimal toBigDecimal(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toBigDecimal(oid, new String(value, encoding));
    }

    public Double toDouble(Oid oid, byte[] value) {
        return value == null ? null : NumericConversions.toDouble(oid, new String(value, encoding));
    }

    public Date toDate(Oid oid, byte[] value) {
        return value == null ? null : TemporalConversions.toDate(oid, new String(value, encoding));
    }

    public Time toTime(Oid oid, byte[] value) {
        return value == null ? null : TemporalConversions.toTime(oid, new String(value, encoding));
    }

    public Timestamp toTimestamp(Oid oid, byte[] value) {
        return value == null ? null : TemporalConversions.toTimestamp(oid, new String(value, encoding));
    }

    public byte[] toBytes(Oid oid, byte[] value) {
        return value == null ? null : BlobConversions.toBytes(oid, new String(value, 2, value.length - 2, encoding));
    }

    public Boolean toBoolean(Oid oid, byte[] value) {
        return value == null ? null : BooleanConversions.toBoolean(oid, new String(value, encoding));
    }

    public <TArray> TArray toArray(Class<TArray> arrayType, Oid oid, byte[] value) {
        if (value == null) {
            return null;
        }
        String svalue = new String(value, encoding);
        switch (oid) {
            case INT2_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, NumericConversions::toShort);
            case INT4_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, NumericConversions::toInteger);
            case INT8_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, NumericConversions::toLong);

            case TEXT_ARRAY:
            case CHAR_ARRAY:
            case BPCHAR_ARRAY:
            case VARCHAR_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, StringConversions::toString);

            case NUMERIC_ARRAY:
            case FLOAT4_ARRAY:
            case FLOAT8_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, NumericConversions::toBigDecimal);

            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, TemporalConversions::toTimestamp);

            case TIMETZ_ARRAY:
            case TIME_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, TemporalConversions::toTime);

            case DATE_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, TemporalConversions::toDate);

            case BOOL_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, BooleanConversions::toBoolean);
            case BYTEA_ARRAY:
                return ArrayConversions.toArray(arrayType, oid, svalue, (oide, svaluee) -> {
                    byte[] first = BlobConversions.toBytes(oide, svaluee.substring(2));
                    return parseHexBinary(new String(first, 1, first.length - 1, encoding));
                });
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
        return value == null ? null : (T) converter.to(oid, new String(value, encoding));
    }

    private String fromObject(Object o) {
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
            return o.toString();
        }
        return fromConvertible(o);
    }

    @SuppressWarnings("unchecked")
    private String fromConvertible(Object value) {
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
            String converted = fromObject(param);
            params[i++] = converted == null ? null : converted.getBytes(encoding);
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
                return toConvertible(oid, value);
        }
    }

    private Object toConvertible(Oid oid, byte[] value) {
        throw new IllegalStateException("Unknown conversion source: " + oid);
    }

    public Oid[] assumeTypes(Object... params) {
        Oid[] types = new Oid[params.length];
        for (int i = 0; i < params.length; i++) {
            if (params[i] instanceof Double) {
                types[i] = Oid.FLOAT8;
            } else if (params[i] instanceof double[]) {
                types[i] = Oid.FLOAT8_ARRAY;
            } else if (params[i] instanceof Float) {
                types[i] = Oid.FLOAT4;
            } else if (params[i] instanceof float[]) {
                types[i] = Oid.FLOAT4_ARRAY;
            } else if (params[i] instanceof Long) {
                types[i] = Oid.INT8;
            } else if (params[i] instanceof long[]) {
                types[i] = Oid.INT8_ARRAY;
            } else if (params[i] instanceof Integer) {
                types[i] = Oid.INT4;
            } else if (params[i] instanceof int[]) {
                types[i] = Oid.INT4_ARRAY;
            } else if (params[i] instanceof Short) {
                types[i] = Oid.INT2;
            } else if (params[i] instanceof short[]) {
                types[i] = Oid.INT2_ARRAY;
            } else if (params[i] instanceof Byte) {
                types[i] = Oid.INT2;
            } else if (params[i] instanceof byte[]) {
                types[i] = Oid.BYTEA;
            } else if (params[i] instanceof byte[][]) {
                types[i] = Oid.BYTEA_ARRAY;
            } else if (params[i] instanceof BigInteger) {
                types[i] = Oid.NUMERIC;
            } else if (params[i] instanceof BigInteger[]) {
                types[i] = Oid.NUMERIC_ARRAY;
            } else if (params[i] instanceof BigDecimal) {
                types[i] = Oid.NUMERIC;
            } else if (params[i] instanceof BigDecimal[]) {
                types[i] = Oid.NUMERIC_ARRAY;
            } else if (params[i] instanceof Boolean) {
                types[i] = Oid.BOOL;
            } else if (params[i] instanceof Boolean[]) {
                types[i] = Oid.BOOL_ARRAY;
            } else if (params[i] instanceof CharSequence) {
                types[i] = Oid.VARCHAR;
            } else if (params[i] instanceof Character) {
                types[i] = Oid.VARCHAR;
            } else if (params[i] instanceof Date) {
                types[i] = Oid.TIMESTAMP;
            } else if (params[i] instanceof Timestamp) {
                types[i] = Oid.TIMESTAMP;
            } else if (params[i] instanceof Instant) {
                types[i] = Oid.TIMESTAMP;
            } else if (params[i] instanceof OffsetDateTime) {
                types[i] = Oid.TIMESTAMP;
            } else if (params[i] instanceof LocalDateTime) {
                types[i] = Oid.TIMESTAMPTZ;
            } else if (params[i] instanceof Time) {
                types[i] = Oid.TIME;
            } else if (params[i] instanceof OffsetTime) {
                types[i] = Oid.TIME;
            } else if (params[i] instanceof LocalTime) {
                types[i] = Oid.TIMETZ;
            } else if (params[i] instanceof UUID) {
                types[i] = Oid.UUID;
            } else {
                types[i] = Oid.UNSPECIFIED;
            }
        }
        return types;
    }

}
