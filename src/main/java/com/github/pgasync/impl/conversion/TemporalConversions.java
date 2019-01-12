package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * @author Antti Laisi
 * <p>
 * TODO: Add support for Java 8 temporal types.
 */
class TemporalConversions {

    private static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .toFormatter();

    private static final DateTimeFormatter TIMESTAMPZ_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "")
            .toFormatter();

    private static final DateTimeFormatter TIMEZ_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "")
            .toFormatter();

    static Date toDate(Oid oid, byte[] value, Charset charset) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case DATE:
                String date = new String(value, charset);
                try {
                    return Date.valueOf(LocalDate.parse(date, ISO_LOCAL_DATE));
                } catch (DateTimeParseException e) {
                    throw new SqlException("Invalid date: " + date);
                }
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Date");
        }
    }

    static Time toTime(Oid oid, byte[] value, Charset charset) {
        String time = new String(value, charset);
        try {
            switch (oid) {
                case UNSPECIFIED: // fallthrough
                case TIME:
                    return Time.valueOf(LocalTime.parse(time, ISO_LOCAL_TIME));
                case TIMETZ:
                    return Time.valueOf(OffsetTime.parse(time, TIMEZ_FORMAT).toLocalTime());
                default:
                    throw new SqlException("Unsupported conversion " + oid.name() + " -> Time");
            }
        } catch (DateTimeParseException e) {
            throw new SqlException("Invalid time: " + time);
        }
    }

    static Timestamp toTimestamp(Oid oid, byte[] value, Charset charset) {
        String time = new String(value, charset);
        try {
            switch (oid) {
                case UNSPECIFIED: // fallthrough
                case TIMESTAMP:
                    return Timestamp.valueOf(LocalDateTime.parse(time, TIMESTAMP_FORMAT));
                case TIMESTAMPTZ:
                    return Timestamp.valueOf(OffsetDateTime.parse(time, TIMESTAMPZ_FORMAT).toLocalDateTime());
                default:
                    throw new SqlException("Unsupported conversion " + oid.name() + " -> Time");
            }
        } catch (DateTimeParseException e) {
            throw new SqlException("Invalid time: " + time);
        }
    }

    static byte[] fromTime(Time time, Charset charset) {
        return ISO_LOCAL_TIME.format(time.toLocalTime()).getBytes(charset);
    }

    static byte[] fromDate(Date date, Charset charset) {
        return ISO_LOCAL_DATE.format(date.toLocalDate()).getBytes(charset);
    }

    static byte[] fromTimestamp(Timestamp ts, Charset charset) {
        return TIMESTAMP_FORMAT.format(ts.toLocalDateTime()).getBytes(charset);
    }
}
