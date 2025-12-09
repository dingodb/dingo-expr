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

package io.dingodb.expr.common.timezone.converter;

import io.dingodb.expr.common.timezone.core.DingoDateTime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * Safe output formatting to avoid JVM time zone effects.
 */
public class SafeOutputFormatter {

    public String toSafeString(Object sqlDateTime, ZoneId displayZone) {
        if (sqlDateTime == null) {
            return null;
        }

        if (sqlDateTime instanceof java.sql.Timestamp) {
            return formatTimestamp((java.sql.Timestamp) sqlDateTime, displayZone);
        } else if (sqlDateTime instanceof java.sql.Date) {
            return formatDate((java.sql.Date) sqlDateTime, displayZone);
        } else if (sqlDateTime instanceof java.sql.Time) {
            return formatTime((java.sql.Time) sqlDateTime, displayZone);
        } else if (sqlDateTime instanceof java.util.Date) {
            return formatUtilDate((java.util.Date) sqlDateTime, displayZone);
        } else {
            return sqlDateTime.toString();
        }
    }

    private String formatTimestamp(java.sql.Timestamp timestamp, ZoneId displayZone) {
        Instant instant = timestamp.toInstant();
        return instant.atZone(displayZone)
            .format(new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME)
                .toFormatter());
    }

    private String formatDate(java.sql.Date date, ZoneId displayZone) {
        return date.toLocalDate()
            .format(ISO_LOCAL_DATE);
    }

    private String formatTime(java.sql.Time time, ZoneId displayZone) {
        return time.toLocalTime()
            .format(ISO_LOCAL_TIME);
    }

    private String formatUtilDate(java.util.Date date, ZoneId displayZone) {
        Instant instant = date.toInstant();
        return instant.atZone(displayZone)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    public String toFormattedString(Object sqlDateTime, String pattern, ZoneId displayZone) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);

        if (sqlDateTime instanceof java.sql.Timestamp) {
            Instant instant = ((java.sql.Timestamp) sqlDateTime).toInstant();
            return instant.atZone(displayZone).format(formatter);
        } else if (sqlDateTime instanceof java.sql.Date) {
            return ((java.sql.Date) sqlDateTime).toLocalDate().format(formatter);
        } else if (sqlDateTime instanceof java.sql.Time) {
            return ((java.sql.Time) sqlDateTime).toLocalTime().format(formatter);
        } else if (sqlDateTime instanceof java.util.Date) {
            Instant instant = ((java.util.Date) sqlDateTime).toInstant();
            return instant.atZone(displayZone).format(formatter);
        }

        return sqlDateTime.toString();
    }

    public String formatDingoDateTime(DingoDateTime dingoDateTime, ZoneId displayZone) {
        if (dingoDateTime == null) {
            return null;
        }

        if (!dingoDateTime.isTimeZoneSensitive()) {
            Object value = dingoDateTime.getValue();
            if (value instanceof java.time.LocalDate) {
                return ((java.time.LocalDate) value).format(ISO_LOCAL_DATE);
            } else if (value instanceof java.time.LocalTime) {
                return ((java.time.LocalTime) value).format(ISO_LOCAL_TIME);
            } else if (value instanceof java.time.LocalDateTime) {
                return ((java.time.LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            }
        } else {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dingoDateTime;
            return tzValue.getUtcValue()
                .atZone(displayZone)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }

        return dingoDateTime.toString();
    }
}
