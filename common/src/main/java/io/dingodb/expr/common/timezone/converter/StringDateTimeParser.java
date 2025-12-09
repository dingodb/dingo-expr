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

import io.dingodb.expr.common.timezone.DateTimeUtils;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

public class StringDateTimeParser {

    private final DateTimeFormatter[] dateFormatters = DateTimeUtils.DEFAULT_PARSE_DATE_FORMATTERS;
    private final DateTimeFormatter[] timeFormatters = DateTimeUtils.DEFAULT_PARSE_TIME_FORMATTERS;
    private final DateTimeFormatter[] timestampFormatters = DateTimeUtils.DEFAULT_PARSE_TIMESTAMP_FORMATTERS;
    private final DateTimeFormatter[] timestampTzFormatters = DateTimeUtils.DEFAULT_PARSE_TIMESTAMP_TZ_FORMATTERS;

    public StringDateTimeParser() {
    }

    public DingoDateTime parseString(String input, DateTimeType targetType, ZoneId defaultZone) {
        Objects.requireNonNull(input, "Input string cannot be null");
        Objects.requireNonNull(targetType, "Target type cannot be null");

        try {
            switch (targetType) {
                case DATE:
                    return parseToDate(input);
                case TIME:
                    return parseToTime(input);
                case TIMESTAMP:
                    return parseToTimestamp(input);
                case TIMESTAMP_TZ:
                    return parseToTimestampTZ(input, defaultZone);
                default:
                    throw new IllegalArgumentException("Unsupported target type: " + targetType);
            }
        } catch (DateTimeParseException e) {
            throw new DateTimeParseException(
                "Failed to parse '" + input + "' as " + targetType, input, 0, e);
        }
    }

    private DingoDateTime.DingoLocalDate parseToDate(String input) {
        for (DateTimeFormatter formatter : dateFormatters) {
            try {
                LocalDate date = LocalDate.parse(input, formatter);
                return new DingoDateTime.DingoLocalDate(date);
            } catch (DateTimeParseException ignore) {
                // ignore
            }
        }
        return null;
    }

    private DingoDateTime.DingoLocalTime parseToTime(String input) {
        for (DateTimeFormatter formatter : timeFormatters) {
            try {
                LocalTime time = LocalTime.parse(input, formatter);
                return new DingoDateTime.DingoLocalTime(time);
            } catch (DateTimeParseException ignore) {
                // ignore
            }
        }
        return null;
    }

    private DingoDateTime.DingoLocalDateTime parseToTimestamp(String input) {
        for (DateTimeFormatter formatter : timestampFormatters) {
            try {
                LocalDateTime datetime = LocalDateTime.parse(input, formatter);
                return new DingoDateTime.DingoLocalDateTime(datetime);
            } catch (DateTimeParseException ignore) {
                // ignore
            }
        }

        try {
            String[] parts = input.split(" ");
            if (parts.length == 2) {
                LocalDate date = parseToDate(parts[0]).getValue();
                LocalTime time = parseToTime(parts[1]).getValue();
                return new DingoDateTime.DingoLocalDateTime(LocalDateTime.of(date, time));
            }
        } catch (Exception ignore) {
            // ignore
        }

        return null;
    }

    private DingoDateTime.DingoTimestampTZ parseToTimestampTZ(String input, ZoneId defaultZone) {
        for (DateTimeFormatter formatter : timestampTzFormatters) {
            try {
                if (formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME || formatter.toString().contains("XXX")) {
                    OffsetDateTime odt = OffsetDateTime.parse(input, formatter);
                    return new DingoDateTime.DingoTimestampTZ(odt.toInstant(), odt.getOffset());
                } else {
                    ZonedDateTime zdt = ZonedDateTime.parse(input, formatter);
                    return new DingoDateTime.DingoTimestampTZ(zdt.toInstant(), zdt.getZone());
                }
            } catch (DateTimeParseException ignore) {
                // ignore
            }
        }

        try {
            LocalDateTime ldt = parseToTimestamp(input).getValue();
            ZonedDateTime zdt = ldt.atZone(defaultZone);
            return new DingoDateTime.DingoTimestampTZ(zdt.toInstant(), defaultZone);
        } catch (DateTimeParseException e) {
            throw new DateTimeParseException(
                "No suitable timestamp with time zone format found for: " + input, input, 0);
        }
    }
}
