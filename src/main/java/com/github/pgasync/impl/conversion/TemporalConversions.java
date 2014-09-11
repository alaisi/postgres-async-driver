package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Antti Laisi
 *
 * TODO: Add support for Java 8 temporal types and use new parsers.
 */
enum TemporalConversions {
    ;

    static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return zoned(new SimpleDateFormat("yyyy-MM-dd"));
        }
    };
    static final ThreadLocal<DateFormat> TIME_FORMAT = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return zoned(new SimpleDateFormat("HH:mm:ss.SSS"));
        }
    };
    static final ThreadLocal<DateFormat> TIME_FORMAT_NO_MILLIS = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return zoned(new SimpleDateFormat("HH:mm:ss"));
        }
    };
    static final ThreadLocal<DateFormat> TIMESTAMP_FORMAT = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return zoned(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
        }
    };

    static Date toDate(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case DATE:
                String date = new String(value, UTF_8);
                try {
                    return new Date(DATE_FORMAT.get().parse(date).getTime());
                } catch (ParseException e) {
                    throw new SqlException("Invalid date: " + date);
                }
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Date");
        }
    }

    static Time toTime(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case TIMETZ: // fallthrough
            case TIME:
                String time = new String(value, UTF_8);
                try {
                    DateFormat format = time.length() == 8 ? TIME_FORMAT_NO_MILLIS.get() : TIME_FORMAT.get();
                    return new Time(format.parse(time).getTime());
                } catch (ParseException e) {
                    throw new SqlException("Invalid time: " + time);
                }
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Time");
        }
    }

    static Timestamp toTimestamp(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case TIMESTAMP: // fallthrough
            case TIMESTAMPTZ:
                String time = new String(value, UTF_8);
                try {
                    return new Timestamp(TIMESTAMP_FORMAT.get().parse(time).getTime());
                } catch (ParseException e) {
                    throw new SqlException("Invalid time: " + time);
                }
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Time");
        }
    }

    static byte[] fromTime(Time time) {
        return TIME_FORMAT.get().format(time).getBytes(UTF_8);
    }

    static byte[] fromDate(Date date) {
        return DATE_FORMAT.get().format(date).getBytes(UTF_8);
    }

    private static SimpleDateFormat zoned(SimpleDateFormat format) {
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        return format;
    }

}
