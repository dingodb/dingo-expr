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

package io.dingodb.expr.common.timezone.processor;

import io.dingodb.expr.common.timezone.InputTypeAnalyzer;
import io.dingodb.expr.common.timezone.converter.StringDateTimeParser;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class SmartInputProcessor {

    private final ZoneId defaultZone;
    private final StringDateTimeParser stringParser;

    public SmartInputProcessor(ZoneId defaultZone) {
        this.defaultZone = defaultZone;
        this.stringParser = new StringDateTimeParser();
    }

    public DingoDateTime processInput(Object input, DateTimeType targetType) {
        if (input == null) {
            return null;
        }

        InputTypeAnalyzer.TimeZoneSensitivity sensitivity = InputTypeAnalyzer.analyzeSensitivity(
            input, targetType, defaultZone);

        switch (sensitivity) {
            case SENSITIVE:
                return processTimeZoneSensitiveInput(input, targetType);
            case INSENSITIVE:
                return processTimeZoneInsensitiveInput(input, targetType);
            case AMBIGUOUS:
                return processAmbiguousInput(input, targetType);
            default:
                return processUnknownInput(input, targetType);
        }
    }

    private DingoDateTime processTimeZoneSensitiveInput(Object input, DateTimeType targetType) {
        if (input instanceof String) {
            return stringParser.parseString((String) input, DateTimeType.TIMESTAMP_TZ, defaultZone);
        } else if (input instanceof java.sql.Timestamp) {
            java.sql.Timestamp ts = (java.sql.Timestamp) input;
            return new DingoDateTime.DingoTimestampTZ(
                ts.toInstant(), defaultZone);
        } else if (input instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) input;
            return new DingoDateTime.DingoTimestampTZ(
                date.toInstant(), defaultZone);
        } else if (input instanceof DingoDateTime) {
            DingoDateTime dingoDateTime = (DingoDateTime) input;
            if (dingoDateTime.isTimeZoneSensitive()) {
                return dingoDateTime;
            } else {
                return convertToTimeZoneSensitive(dingoDateTime);
            }
        }

        throw new IllegalArgumentException("Unsupported timezone-sensitive input: " + input.getClass());
    }

    private DingoDateTime processTimeZoneInsensitiveInput(Object input, DateTimeType targetType) {
        if (input instanceof String) {
            return stringParser.parseString((String) input, targetType, defaultZone);
        } else if (input instanceof java.sql.Date) {
            java.sql.Date sqlDate = (java.sql.Date) input;
            if (targetType == DateTimeType.DATE) {
                return new DingoDateTime.DingoLocalDate(sqlDate.toLocalDate());
            }
        } else if (input instanceof java.sql.Time) {
            java.sql.Time sqlTime = (java.sql.Time) input;
            if (targetType == DateTimeType.TIME) {
                return new DingoDateTime.DingoLocalTime(sqlTime.toLocalTime());
            }
        } else if (input instanceof DingoDateTime) {
            DingoDateTime dingoDateTime = (DingoDateTime) input;
            if (!dingoDateTime.isTimeZoneSensitive()) {
                return dingoDateTime;
            }
        }

        DingoDateTime sensitive = processTimeZoneSensitiveInput(input, DateTimeType.TIMESTAMP_TZ);
        return convertToTimeZoneInsensitive(sensitive, targetType);
    }

    private DingoDateTime processAmbiguousInput(Object input, DateTimeType targetType) {
        if (targetType.isTimeZoneSensitive()) {
            return processTimeZoneSensitiveInput(input, targetType);
        } else {
            return processTimeZoneInsensitiveInput(input, targetType);
        }
    }

    private DingoDateTime processUnknownInput(Object input, DateTimeType targetType) {
        try {
            return processTimeZoneInsensitiveInput(input.toString(), targetType);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot process unknown input type: " + input.getClass(), e);
        }
    }

    private DingoDateTime convertToTimeZoneSensitive(DingoDateTime insensitive) {
        if (insensitive.isTimeZoneSensitive()) {
            return insensitive;
        }

        if (insensitive instanceof DingoDateTime.DingoLocalDate) {
            DingoDateTime.DingoLocalDate localDate = (DingoDateTime.DingoLocalDate) insensitive;
            LocalDateTime localDateTime = localDate.getValue().atStartOfDay();
            ZonedDateTime zdt = localDateTime.atZone(defaultZone);
            return new DingoDateTime.DingoTimestampTZ(zdt.toInstant(), defaultZone);
        } else if (insensitive instanceof DingoDateTime.DingoLocalTime) {
            DingoDateTime.DingoLocalTime localTime = (DingoDateTime.DingoLocalTime) insensitive;
            LocalDateTime localDateTime = LocalDate.now().atTime(localTime.getValue());
            ZonedDateTime zdt = localDateTime.atZone(defaultZone);
            return new DingoDateTime.DingoTimestampTZ(zdt.toInstant(), defaultZone);
        } else if (insensitive instanceof DingoDateTime.DingoLocalDateTime) {
            DingoDateTime.DingoLocalDateTime localDateTime = (DingoDateTime.DingoLocalDateTime) insensitive;
            ZonedDateTime zdt = localDateTime.getValue().atZone(defaultZone);
            return new DingoDateTime.DingoTimestampTZ(zdt.toInstant(), defaultZone);
        }

        throw new IllegalArgumentException("Unsupported insensitive type: " + insensitive.getClass());
    }

    private DingoDateTime convertToTimeZoneInsensitive(DingoDateTime sensitive, DateTimeType targetType) {
        if (!sensitive.isTimeZoneSensitive()) {
            return sensitive;
        }

        DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) sensitive;

        switch (targetType) {
            case DATE:
                LocalDate localDate = tzValue.getUtcValue()
                    .atZone(tzValue.getOriginalZone())
                    .toLocalDate();
                return new DingoDateTime.DingoLocalDate(localDate);
            case TIME:
                LocalTime localTime = tzValue.getUtcValue()
                    .atZone(tzValue.getOriginalZone())
                    .toLocalTime();
                return new DingoDateTime.DingoLocalTime(localTime);
            case TIMESTAMP:
                LocalDateTime localDateTime = tzValue.getUtcValue()
                    .atZone(tzValue.getOriginalZone())
                    .toLocalDateTime();
                return new DingoDateTime.DingoLocalDateTime(localDateTime);
            default:
                throw new IllegalArgumentException("Unsupported target type: " + targetType);
        }
    }
}
