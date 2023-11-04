/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.expr.runtime.utils;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.util.Arrays;
import java.util.TimeZone;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

public final class DateTimeUtils {
    public static final DateTimeFormatter DATE_SEP_BY_HYPHEN = dateFormatterWithSeparator('-');
    public static final DateTimeFormatter DATE_SEP_BY_SLASH = dateFormatterWithSeparator('/');
    public static final DateTimeFormatter DATE_SEP_BY_DOT = dateFormatterWithSeparator('.');
    public static final DateTimeFormatter DATE_NO_SEP = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendValue(YEAR, 4)
        .appendValue(MONTH_OF_YEAR, 2)
        .appendValue(DAY_OF_MONTH, 2)
        .toFormatter()
        .withResolverStyle(ResolverStyle.STRICT);

    public static final DateTimeFormatter TIME_SEP_BY_COLON = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NEVER)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 0, 3, true)
        .toFormatter()
        .withResolverStyle(ResolverStyle.STRICT);
    public static final DateTimeFormatter TIME_NO_SEP = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendValue(HOUR_OF_DAY, 2)
        .appendValue(MINUTE_OF_HOUR, 2)
        .appendValue(SECOND_OF_MINUTE, 2)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 0, 3, true)
        .toFormatter()
        .withResolverStyle(ResolverStyle.STRICT);

    public static final DateTimeFormatter DATE_TIME_SEP_BY_HYPHEN_COLON =
        concatDateTimeFormatter(DATE_SEP_BY_HYPHEN, TIME_SEP_BY_COLON, ' ');
    public static final DateTimeFormatter DATE_TIME_SEP_BY_SLASH_COLON =
        concatDateTimeFormatter(DATE_SEP_BY_SLASH, TIME_SEP_BY_COLON, ' ');
    public static final DateTimeFormatter DATE_TIME_SEP_BY_DOT_COLON =
        concatDateTimeFormatter(DATE_SEP_BY_DOT, TIME_SEP_BY_COLON, ' ');
    public static final DateTimeFormatter DATE_TIME_NO_SEP =
        concatDateTimeFormatter(DATE_NO_SEP, TIME_NO_SEP, null);
    public static final DateTimeFormatter[] DEFAULT_IN_TIMESTAMP_FORMATTERS = new DateTimeFormatter[]{
        DateTimeUtils.DATE_TIME_SEP_BY_HYPHEN_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_SLASH_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_DOT_COLON,
        DateTimeUtils.DATE_TIME_NO_SEP,
    };
    public static final DateTimeFormatter[] DEFAULT_IN_DATE_FORMATTERS = new DateTimeFormatter[]{
        DateTimeUtils.DATE_SEP_BY_HYPHEN,
        DateTimeUtils.DATE_SEP_BY_SLASH,
        DateTimeUtils.DATE_SEP_BY_DOT,
        DateTimeUtils.DATE_NO_SEP,
    };
    public static final DateTimeFormatter[] DEFAULT_IN_TIME_FORMATTERS = new DateTimeFormatter[]{
        DateTimeUtils.TIME_SEP_BY_COLON,
        DateTimeUtils.TIME_NO_SEP,
    };
    public static final DateTimeFormatter DEFAULT_OUT_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    public static final DateTimeFormatter DEFAULT_OUT_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;
    public static final DateTimeFormatter DEFAULT_OUT_TIMESTAMP_FORMATTER =
        concatDateTimeFormatter(DateTimeFormatter.ISO_LOCAL_DATE, DateTimeFormatter.ISO_LOCAL_TIME, ' ');

    private static final long ONE_DAY_IN_MILLI = 24L * 60L * 60L * 1000L;

    private DateTimeUtils() {
    }

    private static @NonNull DateTimeFormatter dateFormatterWithSeparator(char sep) {
        return new DateTimeFormatterBuilder().parseCaseInsensitive()
            .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral(sep)
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
            .appendLiteral(sep)
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);
    }

    private static @NonNull DateTimeFormatter concatDateTimeFormatter(
        DateTimeFormatter dateFormatter,
        DateTimeFormatter timeFormatter,
        Character sep
    ) {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        builder.append(dateFormatter);
        if (sep != null) {
            builder.appendLiteral(sep);
        }
        builder.append(timeFormatter);
        return builder.toFormatter().withResolverStyle(ResolverStyle.STRICT);
    }

    public static @Nullable Date parseDate(@NonNull String value) {
        return parseDate(value, DEFAULT_IN_DATE_FORMATTERS);
    }

    /**
     * Parse a {@link String} to {@link Date}.
     *
     * @param value          the input string
     * @param dateFormatters date formatters to try
     * @return the date
     */
    public static @Nullable Date parseDate(
        @NonNull String value,
        @NonNull DateTimeFormatter @NonNull [] dateFormatters
    ) {
        if (value.isEmpty()) {
            return null;
        }
        for (DateTimeFormatter dtf : dateFormatters) {
            try {
                LocalDateTime t = LocalDate.parse(value, dtf).atStartOfDay();
                return new Date(t.toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse date string \"" + value + "\", supported formats are "
                + Arrays.toString(dateFormatters) + "."
        );
    }

    public static @Nullable Time parseTime(@NonNull String value) {
        return parseTime(value, DEFAULT_IN_TIME_FORMATTERS);
    }

    /**
     * Parse a {@link String} to {@link Time}.
     *
     * @param value          the input string
     * @param timeFormatters time formatters to try
     * @return the time
     */
    public static @Nullable Time parseTime(
        @NonNull String value,
        @NonNull DateTimeFormatter @NonNull [] timeFormatters
    ) {
        if (value.isEmpty()) {
            return null;
        }
        for (DateTimeFormatter dtf : timeFormatters) {
            try {
                LocalDateTime t = LocalTime.parse(value, dtf).atDate(LocalDate.of(1970, 1, 1));
                return new Time(t.toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse time string \"" + value + "\", supported formats are "
                + Arrays.toString(timeFormatters) + "."
        );
    }

    public static Timestamp parseTimestamp(@NonNull String value) {
        return parseTimestamp(value, DEFAULT_IN_TIMESTAMP_FORMATTERS);
    }

    /**
     * Parse a {@link String} to {@link Timestamp}.
     *
     * @param value               the input string
     * @param timestampFormatters timestamp formatters to try
     * @return the timestamp
     */
    public static @Nullable Timestamp parseTimestamp(
        @NonNull String value,
        @NonNull DateTimeFormatter @NonNull [] timestampFormatters
    ) {
        if (value.isEmpty()) {
            return null;
        }
        for (DateTimeFormatter dtf : timestampFormatters) {
            try {
                LocalDateTime t = LocalDateTime.parse(value, dtf);
                return Timestamp.valueOf(t);
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse timestamp string \"" + value + "\", supported formats are "
                + Arrays.toString(timestampFormatters) + "."
        );
    }

    public static @NonNull ZonedDateTime toUtcTime(long milliSeconds) {
        return Instant.ofEpochMilli(milliSeconds).atZone(ZoneOffset.UTC);
    }

    // For testing and debugging
    public static @NonNull String toUtcString(long milliSeconds) {
        final DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dtf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dtf.format(new Timestamp(milliSeconds));
    }

    public static @NonNull BigDecimal toSecond(long milliSeconds, int scale) {
        return BigDecimal.valueOf(milliSeconds)
            .divide(BigDecimal.valueOf(1000L), scale, RoundingMode.HALF_UP);
    }

    public static long fromSecond(@NonNull BigDecimal second) {
        return second.multiply(BigDecimal.valueOf(1000L))
            .setScale(0, RoundingMode.HALF_UP)
            .longValue();
    }

    public static long fromSecond(long second) {
        return second * 1000L;
    }

    public static @NonNull String dateFormat(@NonNull Date value) {
        return toUtcTime(value.getTime()).format(DEFAULT_OUT_DATE_FORMATTER);
    }

    public static @NonNull String dateFormat(@NonNull Date value, String format) {
        return toUtcTime(value.getTime()).format(
            DateTimeFormatter.ofPattern(format).withResolverStyle(ResolverStyle.STRICT)
        );
    }

    public static @NonNull String timeFormat(@NonNull Time value) {
        return toUtcTime(value.getTime()).format(DEFAULT_OUT_TIME_FORMATTER);
    }

    public static @NonNull String timeFormat(@NonNull Time value, String format) {
        return toUtcTime(value.getTime()).format(
            DateTimeFormatter.ofPattern(format).withResolverStyle(ResolverStyle.STRICT)
        );
    }

    public static @NonNull String timestampFormat(@NonNull Timestamp value) {
        return value.toLocalDateTime().format(DEFAULT_OUT_TIMESTAMP_FORMATTER);
    }

    public static @NonNull String timestampFormat(@NonNull Timestamp value, String format) {
        return value.toLocalDateTime().format(
            DateTimeFormatter.ofPattern(format).withResolverStyle(ResolverStyle.STRICT)
        );
    }

    public static @NonNull Date currentDate(@NonNull TimeZone timeZone) {
        long millis = System.currentTimeMillis();
        millis = Math.floorDiv(
            millis + timeZone.getOffset(millis),
            ONE_DAY_IN_MILLI
        ) * ONE_DAY_IN_MILLI;
        return new Date(millis);
    }

    public static @NonNull Time currentTime(@NonNull TimeZone timeZone) {
        long millis = System.currentTimeMillis();
        millis = Math.floorMod(
            millis + timeZone.getOffset(millis),
            ONE_DAY_IN_MILLI
        );
        return new Time(millis);
    }

    public static @NonNull Timestamp currentTimestamp() {
        long millis = System.currentTimeMillis();
        return new Timestamp(millis);
    }
}
