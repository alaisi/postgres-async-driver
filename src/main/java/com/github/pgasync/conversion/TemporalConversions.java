package com.github.pgasync.conversion;

import com.pgasync.SqlException;
import com.github.pgasync.Oid;

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

    static Date toDate(Oid oid, String value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case DATE:
                try {
                    return Date.valueOf(LocalDate.parse(value, ISO_LOCAL_DATE));
                } catch (DateTimeParseException e) {
                    throw new SqlException("Invalid date: " + value);
                }
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Date");
        }
    }

    static Time toTime(Oid oid, String value) {
        try {
            switch (oid) {
                case UNSPECIFIED: // fallthrough
                case TIME:
                    return Time.valueOf(LocalTime.parse(value, ISO_LOCAL_TIME));
                case TIMETZ:
                    return Time.valueOf(OffsetTime.parse(value, TIMEZ_FORMAT).toLocalTime());
                default:
                    throw new SqlException("Unsupported conversion " + oid.name() + " -> Time");
            }
        } catch (DateTimeParseException e) {
            throw new SqlException("Invalid time: " + value);
        }
    }

    static Timestamp toTimestamp(Oid oid, String value) {
        try {
            switch (oid) {
                case UNSPECIFIED: // fallthrough
                case TIMESTAMP:
                    return Timestamp.valueOf(LocalDateTime.parse(value, TIMESTAMP_FORMAT));
                case TIMESTAMPTZ:
                    return Timestamp.valueOf(OffsetDateTime.parse(value, TIMESTAMPZ_FORMAT).toLocalDateTime());
                default:
                    throw new SqlException("Unsupported conversion " + oid.name() + " -> Time");
            }
        } catch (DateTimeParseException e) {
            throw new SqlException("Invalid time: " + value);
        }
    }

    static String fromTime(Time time) {
        return ISO_LOCAL_TIME.format(time.toLocalTime());
    }

    static String fromDate(Date date) {
        return ISO_LOCAL_DATE.format(date.toLocalDate());
    }

    static String fromTimestamp(Timestamp ts) {
        return TIMESTAMP_FORMAT.format(ts.toLocalDateTime());
    }
}
