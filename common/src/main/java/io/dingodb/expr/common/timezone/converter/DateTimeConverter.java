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

import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.core.SimpleTimeZoneConfig;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DateTimeConverter {

    private final ZoneId defaultZone;

    public DateTimeConverter(ZoneId defaultZone) {
        this.defaultZone = defaultZone;
    }

    public DateTimeConverter() {
        this(SimpleTimeZoneConfig.getApplicationZone());
    }

    public DingoDateTime convertInput(Object input, DateTimeType targetType) {
        if (input == null || targetType == null) {
            return null;
        }

        if (input instanceof java.sql.Date) {
            return convertSqlDate((java.sql.Date) input, targetType);
        } else if (input instanceof java.sql.Time) {
            return convertSqlTime((java.sql.Time) input, targetType);
        } else if (input instanceof java.sql.Timestamp) {
            return convertSqlTimestamp((java.sql.Timestamp) input, targetType);
        } else if (input instanceof java.util.Date) {
            return convertUtilDate((java.util.Date) input, targetType);
        } else if (input instanceof String) {
            StringDateTimeParser parser = new StringDateTimeParser();
            return parser.parseString((String) input, targetType, defaultZone);
        } else if (input instanceof DingoDateTime) {
            return convertDingoDateTime((DingoDateTime) input, targetType);
        } else {
            throw new IllegalArgumentException("Unsupported input type: " + input.getClass());
        }
    }

    private DingoDateTime convertUtilDate(java.util.Date utilDate, DateTimeType targetType) {
        Instant instant = utilDate.toInstant();

        if (targetType.isTimeZoneSensitive()) {
            return new DingoDateTime.DingoTimestampTZ(instant, defaultZone);
        } else {
            LocalDateTime localDateTime = instant.atZone(defaultZone).toLocalDateTime();

            switch (targetType) {
                case DATE:
                    return new DingoDateTime.DingoLocalDate(localDateTime.toLocalDate());
                case TIME:
                    return new DingoDateTime.DingoLocalTime(localDateTime.toLocalTime());
                case TIMESTAMP:
                    return new DingoDateTime.DingoLocalDateTime(localDateTime);
                default:
                    throw new IllegalArgumentException("Unsupported target type: " + targetType);
            }
        }
    }

    private DingoDateTime convertSqlDate(java.sql.Date sqlDate, DateTimeType targetType) {
        if (targetType.isTimeZoneSensitive()) {
            Instant instant = Instant.ofEpochMilli(sqlDate.getTime());
            return new DingoDateTime.DingoTimestampTZ(instant, defaultZone);
        } else {
            LocalDate localDate = sqlDate.toLocalDate();

            switch (targetType) {
                case DATE:
                    return new DingoDateTime.DingoLocalDate(localDate);
                case TIME:
                    throw new IllegalArgumentException("Cannot convert DATE to TIME");
                case TIMESTAMP:
                    LocalDateTime localDateTime = localDate.atStartOfDay();
                    return new DingoDateTime.DingoLocalDateTime(localDateTime);
                default:
                    throw new IllegalArgumentException("Unsupported target type: " + targetType);
            }
        }
    }

    private DingoDateTime convertSqlTime(java.sql.Time sqlTime, DateTimeType targetType) {
        if (targetType.isTimeZoneSensitive()) {
            Instant instant = Instant.ofEpochMilli(sqlTime.getTime());
            return new DingoDateTime.DingoTimestampTZ(instant, defaultZone);
        } else {
            LocalTime localTime = sqlTime.toLocalTime();

            switch (targetType) {
                case DATE:
                    // throw new IllegalArgumentException("Cannot convert TIME to DATE");
                    return null;
                case TIME:
                    return new DingoDateTime.DingoLocalTime(localTime);
                case TIMESTAMP:
                    /*LocalDateTime localDateTime = LocalDateTime.of(
                        LocalDate.of(1970, 1, 1), localTime);
                    return new DingoDateTime.DingoLocalDateTime(localDateTime);*/
                    return null;
                default:
                    throw new IllegalArgumentException("Unsupported target type: " + targetType);
            }
        }
    }

    private DingoDateTime convertSqlTimestamp(java.sql.Timestamp timestamp, DateTimeType targetType) {
        Instant instant = timestamp.toInstant();

        if (targetType.isTimeZoneSensitive()) {
            return new DingoDateTime.DingoTimestampTZ(instant, defaultZone);
        } else {
            LocalDateTime localDateTime = instant.atZone(defaultZone).toLocalDateTime();

            switch (targetType) {
                case DATE:
                    return new DingoDateTime.DingoLocalDate(localDateTime.toLocalDate());
                case TIME:
                    return new DingoDateTime.DingoLocalTime(localDateTime.toLocalTime());
                case TIMESTAMP:
                    return new DingoDateTime.DingoLocalDateTime(localDateTime);
                default:
                    throw new IllegalArgumentException("Unsupported target type: " + targetType);
            }
        }
    }

    private DingoDateTime convertDingoDateTime(DingoDateTime dingoDateTime, DateTimeType targetType) {
        if (dingoDateTime.getType() == targetType) {
            return dingoDateTime;
        }

        if (dingoDateTime.isTimeZoneSensitive()) {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dingoDateTime;

            if (targetType.isTimeZoneSensitive()) {
                return dingoDateTime;
            } else {
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
        } else {
            if (targetType.isTimeZoneSensitive()) {
                ZoneId targetZone = defaultZone;

                if (dingoDateTime instanceof DingoDateTime.DingoLocalDate) {
                    DingoDateTime.DingoLocalDate localDate = (DingoDateTime.DingoLocalDate) dingoDateTime;
                    LocalDateTime localDateTime = localDate.getValue().atStartOfDay();
                    ZonedDateTime zdt = localDateTime.atZone(targetZone);
                    return new DingoDateTime.DingoTimestampTZ(zdt.toInstant(), targetZone);
                } else if (dingoDateTime instanceof DingoDateTime.DingoLocalTime) {
                    DingoDateTime.DingoLocalTime localTime = (DingoDateTime.DingoLocalTime) dingoDateTime;
                    LocalDateTime localDateTime = LocalDate.now().atTime(localTime.getValue());
                    ZonedDateTime zdt = localDateTime.atZone(targetZone);
                    return new DingoDateTime.DingoTimestampTZ(zdt.toInstant(), targetZone);
                } else if (dingoDateTime instanceof DingoDateTime.DingoLocalDateTime) {
                    DingoDateTime.DingoLocalDateTime localDateTime = (DingoDateTime.DingoLocalDateTime) dingoDateTime;
                    ZonedDateTime zdt = localDateTime.getValue().atZone(targetZone);
                    return new DingoDateTime.DingoTimestampTZ(zdt.toInstant(), targetZone);
                }
            } else {
                if (dingoDateTime instanceof DingoDateTime.DingoLocalDate) {
                    DingoDateTime.DingoLocalDate localDate = (DingoDateTime.DingoLocalDate) dingoDateTime;
                    switch (targetType) {
                        case DATE:
                            return dingoDateTime;
                        case TIMESTAMP:
                            return new DingoDateTime.DingoLocalDateTime(localDate.getValue().atStartOfDay());
                        default:
                            throw new IllegalArgumentException("Unsupported conversion: DATE to " + targetType);
                    }
                } else if (dingoDateTime instanceof DingoDateTime.DingoLocalTime) {
                    DingoDateTime.DingoLocalTime localTime = (DingoDateTime.DingoLocalTime) dingoDateTime;
                    switch (targetType) {
                        case TIME:
                            return dingoDateTime;
                        case TIMESTAMP:
                            LocalDateTime localDateTime = LocalDate.of(1970, 1, 1).atTime(localTime.getValue());
                            return new DingoDateTime.DingoLocalDateTime(localDateTime);
                        default:
                            throw new IllegalArgumentException("Unsupported conversion: TIME to " + targetType);
                    }
                } else if (dingoDateTime instanceof DingoDateTime.DingoLocalDateTime) {
                    DingoDateTime.DingoLocalDateTime localDateTime = (DingoDateTime.DingoLocalDateTime) dingoDateTime;
                    switch (targetType) {
                        case DATE:
                            return new DingoDateTime.DingoLocalDate(localDateTime.getValue().toLocalDate());
                        case TIME:
                            return new DingoDateTime.DingoLocalTime(localDateTime.getValue().toLocalTime());
                        case TIMESTAMP:
                            return dingoDateTime;
                        default:
                            throw new IllegalArgumentException("Unsupported conversion: TIMESTAMP to " + targetType);
                    }
                }
            }
        }

        throw new IllegalArgumentException("Unsupported DingoDateTime conversion: "
                                           + dingoDateTime.getType() + " to " + targetType);
    }
}
