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

package io.dingodb.expr.common.timezone;

import io.dingodb.expr.common.timezone.core.DateTimeType;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;
import java.util.Locale;
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
    public static final DateTimeFormatter TIME_SEP_BY_COLON_HM = strictFormatter("HH:mm");
    public static final DateTimeFormatter TIME_NO_SEP_HM = strictFormatter("HHmm");
    public static final DateTimeFormatter TIME_NO_SEP_WITH_MILLIS = strictFormatter("HHmmssSSS");
    public static final DateTimeFormatter TIME_SEP_BY_COLON_MICRO = strictFormatter("HH:mm:ss.SSSSSS");

    public static final DateTimeFormatter DATE_TIME_SEP_BY_HYPHEN_COLON =
        concatDateTimeFormatter(DATE_SEP_BY_HYPHEN, TIME_SEP_BY_COLON, ' ');
    public static final DateTimeFormatter DATE_TIME_SEP_BY_SLASH_COLON =
        concatDateTimeFormatter(DATE_SEP_BY_SLASH, TIME_SEP_BY_COLON, ' ');
    public static final DateTimeFormatter DATE_TIME_SEP_BY_DOT_COLON =
        concatDateTimeFormatter(DATE_SEP_BY_DOT, TIME_SEP_BY_COLON, ' ');
    public static final DateTimeFormatter DATE_TIME_NO_SEP =
        concatDateTimeFormatter(DATE_NO_SEP, TIME_NO_SEP, null);
    public static final DateTimeFormatter DATE_TIME_SEP_BY_HYPHEN_COLON_T =
        concatDateTimeFormatter(DATE_SEP_BY_HYPHEN, TIME_SEP_BY_COLON, 'T');
    public static final DateTimeFormatter DATE_TIME_SEP_BY_SLASH_COLON_T =
        concatDateTimeFormatter(DATE_SEP_BY_SLASH, TIME_SEP_BY_COLON, 'T');
    public static final DateTimeFormatter DATE_TIME_SEP_BY_DOT_COLON_T =
        concatDateTimeFormatter(DATE_SEP_BY_DOT, TIME_SEP_BY_COLON, 'T');
    public static final DateTimeFormatter DATE_TIME_HYPHEN_MINUTE = strictFormatter("yyyy-MM-dd HH:mm");
    public static final DateTimeFormatter DATE_TIME_HYPHEN_MILLIS = strictFormatter("yyyy-MM-dd HH:mm:ss.SSS");
    public static final DateTimeFormatter DATE_TIME_HYPHEN_MICRO = strictFormatter("yyyy-MM-dd HH:mm:ss.SSSSSS");
    public static final DateTimeFormatter DATE_TIME_SLASH_MINUTE = strictFormatter("yyyy/MM/dd HH:mm");
    public static final DateTimeFormatter DATE_TIME_SLASH_MILLIS = strictFormatter("yyyy/MM/dd HH:mm:ss.SSS");
    public static final DateTimeFormatter DATE_TIME_COMPACT_SECONDS = strictFormatter("yyyyMMddHHmmss");
    public static final DateTimeFormatter DATE_TIME_COMPACT_MILLIS = strictFormatter("yyyyMMddHHmmssSSS");
    public static final DateTimeFormatter DATE_TIME_T_SECONDS = strictFormatter("yyyy-MM-dd'T'HH:mm:ss");
    public static final DateTimeFormatter DATE_TIME_T_MILLIS = strictFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public static final DateTimeFormatter DATE_TIME_T_MICRO = strictFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    public static final DateTimeFormatter[] DEFAULT_PARSE_TIMESTAMP_FORMATTERS = new DateTimeFormatter[]{
        DateTimeFormatter.ISO_LOCAL_DATE_TIME,
        DateTimeUtils.DATE_TIME_SEP_BY_HYPHEN_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_HYPHEN_COLON_T,
        DateTimeUtils.DATE_TIME_T_SECONDS,
        DateTimeUtils.DATE_TIME_T_MILLIS,
        DateTimeUtils.DATE_TIME_T_MICRO,
        DateTimeUtils.DATE_TIME_HYPHEN_MINUTE,
        DateTimeUtils.DATE_TIME_HYPHEN_MILLIS,
        DateTimeUtils.DATE_TIME_HYPHEN_MICRO,
        DateTimeUtils.DATE_TIME_SEP_BY_SLASH_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_SLASH_COLON_T,
        DateTimeUtils.DATE_TIME_SLASH_MINUTE,
        DateTimeUtils.DATE_TIME_SLASH_MILLIS,
        DateTimeUtils.DATE_TIME_SEP_BY_DOT_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_DOT_COLON_T,
        DateTimeUtils.DATE_TIME_NO_SEP,
        DateTimeUtils.DATE_TIME_COMPACT_SECONDS,
        DateTimeUtils.DATE_TIME_COMPACT_MILLIS,
    };
    public static final DateTimeFormatter[] DEFAULT_PARSE_DATE_FORMATTERS = new DateTimeFormatter[]{
        DateTimeFormatter.ISO_LOCAL_DATE,
        DateTimeUtils.DATE_SEP_BY_HYPHEN,
        DateTimeUtils.DATE_SEP_BY_SLASH,
        DateTimeUtils.DATE_SEP_BY_DOT,
        DateTimeUtils.DATE_NO_SEP,
    };
    public static final DateTimeFormatter[] DEFAULT_PARSE_TIME_FORMATTERS = new DateTimeFormatter[]{
        DateTimeUtils.TIME_SEP_BY_COLON,
        DateTimeUtils.TIME_SEP_BY_COLON_HM,
        DateTimeUtils.TIME_SEP_BY_COLON_MICRO,
        DateTimeUtils.TIME_NO_SEP,
        // DateTimeUtils.TIME_NO_SEP_HM,
        DateTimeUtils.TIME_NO_SEP_WITH_MILLIS,
    };
    public static final DateTimeFormatter[] DEFAULT_PARSE_DATE_AND_TIMESTAMP_FORMATTERS = new DateTimeFormatter[]{
        DateTimeUtils.DATE_TIME_SEP_BY_HYPHEN_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_SLASH_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_DOT_COLON,
        DateTimeUtils.DATE_TIME_NO_SEP,
        DateTimeUtils.DATE_SEP_BY_HYPHEN,
        DateTimeUtils.DATE_SEP_BY_SLASH,
        DateTimeUtils.DATE_SEP_BY_DOT,
        DateTimeUtils.DATE_NO_SEP,
    };
    public static final DateTimeFormatter[] DEFAULT_PARSE_TIME_AND_TIMESTAMP_FORMATTERS = new DateTimeFormatter[]{
        DateTimeUtils.TIME_SEP_BY_COLON,
        DateTimeUtils.TIME_NO_SEP,
        DateTimeUtils.DATE_TIME_SEP_BY_HYPHEN_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_SLASH_COLON,
        DateTimeUtils.DATE_TIME_SEP_BY_DOT_COLON,
        DateTimeUtils.DATE_TIME_NO_SEP,
    };
    public static final DateTimeFormatter[] DEFAULT_PARSE_TIMESTAMP_TZ_FORMATTERS = new DateTimeFormatter[]{
        DateTimeFormatter.ISO_OFFSET_DATE_TIME,
        DateTimeFormatter.ISO_ZONED_DATE_TIME,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withResolverStyle(ResolverStyle.STRICT),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX").withResolverStyle(ResolverStyle.STRICT),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX").withResolverStyle(ResolverStyle.STRICT),
    };
    public static final DateTimeFormatter DEFAULT_OUTPUT_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    public static final DateTimeFormatter DEFAULT_OUTPUT_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;
    public static final DateTimeFormatter DEFAULT_OUTPUT_TIMESTAMP_FORMATTER =
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

    private static @NonNull DateTimeFormatter strictFormatter(@NonNull String pattern) {
        return DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.STRICT);
    }

    public static @Nullable DateTimeType analyzeTemporalType(@Nullable String value) {
        return analyzeTemporalType(
            value,
            DEFAULT_PARSE_DATE_FORMATTERS,
            DEFAULT_PARSE_TIME_FORMATTERS,
            DEFAULT_PARSE_TIMESTAMP_FORMATTERS,
            DEFAULT_PARSE_TIMESTAMP_TZ_FORMATTERS
        );
    }

    public static @Nullable DateTimeType analyzeTemporalType(
        @Nullable String value,
        @NonNull DateTimeFormatter @NonNull [] dateFormatters,
        @NonNull DateTimeFormatter @NonNull [] timeFormatters,
        @NonNull DateTimeFormatter @NonNull [] timestampFormatters,
        @NonNull DateTimeFormatter @NonNull [] timestampTzFormatters
    ) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if (matchesTimestampWithZone(trimmed, timestampTzFormatters)) {
            return DateTimeType.TIMESTAMP_TZ;
        }
        if (matchesLocalDateTime(trimmed, timestampFormatters)) {
            return DateTimeType.TIMESTAMP;
        }
        if (matchesLocalDate(trimmed, dateFormatters)) {
            return DateTimeType.DATE;
        }
        if (matchesLocalTime(trimmed, timeFormatters)) {
            return DateTimeType.TIME;
        }
        return null;
    }

    private static boolean matchesLocalDateTime(
        @NonNull String value,
        @NonNull DateTimeFormatter @NonNull [] formatters
    ) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                LocalDateTime.parse(value, formatter);
                return true;
            } catch (DateTimeParseException ignored) {
                // continue
            }
        }
        return false;
    }

    private static boolean matchesTimestampWithZone(
        @NonNull String value,
        @NonNull DateTimeFormatter @NonNull [] formatters
    ) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                if (formatter == DateTimeFormatter.ISO_ZONED_DATE_TIME) {
                    ZonedDateTime.parse(value, formatter);
                } else {
                    OffsetDateTime.parse(value, formatter);
                }
                return true;
            } catch (DateTimeParseException ignored) {
                // continue
            }
        }
        return false;
    }

    private static boolean matchesLocalDate(
        @NonNull String value,
        @NonNull DateTimeFormatter @NonNull [] formatters
    ) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                LocalDate.parse(value, formatter);
                return true;
            } catch (DateTimeParseException ignored) {
                // continue
            }
        }
        return false;
    }

    private static boolean matchesLocalTime(
        @NonNull String value,
        @NonNull DateTimeFormatter @NonNull [] formatters
    ) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                LocalTime.parse(value, formatter);
                return true;
            } catch (DateTimeParseException ignored) {
                // continue
            }
        }
        return false;
    }

    public static @NonNull ZonedDateTime toDefaultTimeZoneTime(long milliSeconds, ZoneId zoneId) {
        return Instant.ofEpochMilli(milliSeconds).atZone(zoneId);
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

    public static @NonNull String dateFormat(@NonNull Date value, @NonNull DateTimeFormatter formatter) {
        return value.toLocalDate().format(formatter);
    }

    public static @NonNull String  dateFormat(@NonNull Date value, @NonNull String format) {
        LocalDate localDate = value.toLocalDate();
        if (format.contains("x")) {
            Integer isoYear = localDate.get(IsoFields.WEEK_BASED_YEAR);
            format = format.replace("x", isoYear.toString());
        }
        if (format.contains("X")) {
            Integer isoYear = localDate.get(IsoFields.WEEK_BASED_YEAR);
            format = format.replace("X", isoYear.toString());
        }
        if (format.contains("v")) {
            Integer isoWeek = localDate.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            if (isoWeek >= 10) {
                format = format.replace("v", isoWeek.toString());
            } else {
                String isoWeekStr = "0" + isoWeek;
                format = format.replace("v", isoWeekStr);
            }
        }
        if (format.contains("V")) {
            Integer isoWeek = localDate.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            if (isoWeek >= 10) {
                format = format.replace("V", isoWeek.toString());
            } else {
                String isoWeekStr = "0" + isoWeek;
                format = format.replace("V", isoWeekStr);
            }
        }
        if (format.contains("'y'")) {
            format = format.replace("'y'", "yy");
        } else if (format.contains("'y-'")) {
            format = format.replace("'y-'", "yy-");
        }
        return localDate.format(DateTimeFormatter.ofPattern(format).withResolverStyle(ResolverStyle.STRICT));
    }

    public static @NonNull String dateFormat(@NonNull Date value) {
        return dateFormat(value, DEFAULT_OUTPUT_DATE_FORMATTER);
    }

    public static String xy(Date value,  String format) {
        LocalDate today = value.toLocalDate();

        WeekFields weekFields = WeekFields.of(Locale.getDefault());

        int weekOfYear = today.get(weekFields.weekOfYear());
        return today.getYear() + "-" + weekOfYear;
    }

    public static @NonNull String timeFormat(@NonNull Time value, @NonNull DateTimeFormatter formatter) {
        return value.toLocalTime().format(formatter);
    }

    public static @NonNull String timeFormat(@NonNull Time value, @NonNull String format) {
        return timeFormat(value, DateTimeFormatter.ofPattern(format).withResolverStyle(ResolverStyle.STRICT));
    }

    public static @NonNull String timeFormat(@NonNull Time value, @NonNull String format, ZoneId zoneId) {
        return timeFormat(value, DateTimeFormatter.ofPattern(format).withResolverStyle(ResolverStyle.STRICT), zoneId);
    }

    public static @NonNull String timeFormat(@NonNull Time value, @NonNull DateTimeFormatter formatter, ZoneId zoneId) {
        return toDefaultTimeZoneTime(value.getTime(), zoneId).format(formatter);
    }

    public static @NonNull String timeFormat(@NonNull Time value) {
        return timeFormat(value, DEFAULT_OUTPUT_TIME_FORMATTER);
    }

    public static @NonNull String timestampFormat(@NonNull Timestamp value) {
        return timestampFormat(value, DEFAULT_OUTPUT_TIMESTAMP_FORMATTER);
    }

    public static @NonNull String timestampFormat(@NonNull Timestamp value, @NonNull DateTimeFormatter formatter) {
        return value.toLocalDateTime().format(formatter);
    }

    public static @NonNull String timestampFormat(@NonNull Timestamp value, @NonNull String format) {
        LocalDateTime localDateTime = value.toLocalDateTime();
        if (format.contains("x")) {
            Integer isoYear = localDateTime.get(IsoFields.WEEK_BASED_YEAR);
            format = format.replace("x", isoYear.toString());
        }
        if (format.contains("X")) {
            Integer isoYear = localDateTime.get(IsoFields.WEEK_BASED_YEAR);
            format = format.replace("X", isoYear.toString());
        }
        if (format.contains("v")) {
            Integer isoWeek = localDateTime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            if (isoWeek >= 10) {
                format = format.replace("v", isoWeek.toString());
            } else {
                String isoWeekStr = "0" + isoWeek;
                format = format.replace("v", isoWeekStr);
            }
        }
        if (format.contains("V")) {
            Integer isoWeek = localDateTime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            if (isoWeek >= 10) {
                format = format.replace("V", isoWeek.toString());
            } else {
                String isoWeekStr = "0" + isoWeek;
                format = format.replace("V", isoWeekStr);
            }
        }
        if (format.contains("'y'")) {
            format = format.replace("'y'", "yy");
        } else if (format.contains("'y-'")) {
            format = format.replace("'y-'", "yy-");
        }
        return localDateTime.format(DateTimeFormatter.ofPattern(format).withResolverStyle(ResolverStyle.STRICT));
    }

    public static int concatIntegers(int[] numbers) {
        int result = 0;
        for (int num : numbers) {
            int temp = num;
            int numDigits = 1;
            while (temp >= 10) {
                numDigits *= 10;
                temp /= 10;
            }
            result = result * numDigits + num;
        }
        return result;
    }

    private static LocalDateTime toLocalDateTime(Object value) {
        if (value instanceof Date) {
            return ((Date) value).toLocalDate().atStartOfDay();
        }
        if (value instanceof Time) {
            java.util.Date date = new java.util.Date(((Time) value).getTime());
            ZonedDateTime zonedDateTime = date.toInstant().atZone(ZoneId.of("UTC"));
            return zonedDateTime.toLocalDateTime();
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toLocalDateTime();
        }
        return null;
    }

    private static LocalDateTime toLocalDateTime(Timestamp value) {
        return value.toLocalDateTime();
    }

    public static @NonNull Timestamp currentTimestampSecond(TimeZone timeZone) {
        ZoneId zoneId = timeZone.toZoneId();
        ZonedDateTime zonedDateTime = ZonedDateTime.now(zoneId);
        ZonedDateTime truncated = zonedDateTime.truncatedTo(ChronoUnit.SECONDS);
        Instant instant = truncated.toInstant();
        Timestamp timestamp = Timestamp.from(instant);
        return timestamp;
    }

    public static long dateDiff(@NonNull Date value0, @NonNull Date value1) {
        return value0.toLocalDate().toEpochDay() - value1.toLocalDate().toEpochDay();
    }

    public static @NonNull String convertFormat(@NonNull String format) {
        StringBuilder builder = new StringBuilder();
        CharacterIterator it = new StringCharacterIterator(format);
        boolean literalStarted = false;
        for (char ch = it.first(); ch != CharacterIterator.DONE; ch = it.next()) {
            if (ch == '%') {
                ch = it.next();
                String fmt = null;
                switch (ch) {
                    case 'Y':
                        fmt = "uuuu";
                        break;
                    case 'm':
                        fmt = "MM";
                        break;
                    case 'd':
                        fmt = "dd";
                        break;
                    case 'H':
                        fmt = "HH";
                        break;
                    case 'i':
                        fmt = "mm";
                        break;
                    case 's':
                    case 'S':
                        fmt = "ss";
                        break;
                    case 'T':
                        fmt = "HH:mm:ss";
                        break;
                    case 'f':
                        fmt = "SSS";
                        break;
                    case CharacterIterator.DONE:
                        continue;
                    default:
                        if (!literalStarted) {
                            builder.append('\'');
                            literalStarted = true;
                        }
                        builder.append(ch);
                        break;
                }
                if (fmt != null) {
                    if (literalStarted) {
                        builder.append('\'');
                        literalStarted = false;
                    }
                    builder.append(fmt);
                }
            } else {
                if (!literalStarted) {
                    builder.append('\'');
                    literalStarted = true;
                }
                builder.append(ch);
            }
        }
        if (literalStarted) {
            builder.append('\'');
        }
        return builder.toString();
    }

}
