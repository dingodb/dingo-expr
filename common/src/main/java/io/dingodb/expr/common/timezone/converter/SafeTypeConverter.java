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

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;

public class SafeTypeConverter {

    private final ZoneId defaultZone;

    public SafeTypeConverter(ZoneId defaultZone) {
        this.defaultZone = defaultZone;
    }

    public SafeTypeConverter() {
        this(SimpleTimeZoneConfig.getApplicationZone());
    }

    /**
     * Convert Timestamp to Date (extract date part).
     */
    public java.sql.Date timestampToDate(DingoDateTime timestamp, ZoneId contextZone) {
        if (timestamp == null) {
            return null;
        }

        if (!timestamp.isTimeZoneSensitive()) {
            if (timestamp instanceof DingoDateTime.DingoLocalDateTime) {
                DingoDateTime.DingoLocalDateTime localDt = (DingoDateTime.DingoLocalDateTime) timestamp;
                LocalDate localDate = localDt.getValue().toLocalDate();
                if (localDate.getYear() < 0) {
                    return null;
                }
                return java.sql.Date.valueOf(localDate);
            } else if (timestamp instanceof DingoDateTime.DingoTimestampTZ) {
                DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) timestamp;
                LocalDate localDate = tzValue.getUtcValue()
                    .atZone(tzValue.getOriginalZone())
                    .toLocalDate();
                if (localDate.getYear() < 0) {
                    return null;
                }
                return java.sql.Date.valueOf(localDate);
            } else {
                throw new IllegalArgumentException("Unsupported timestamp type for date conversion: "
                                                   + timestamp.getClass());
            }
        } else {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) timestamp;
            LocalDate localDate = tzValue.getUtcValue()
                .atZone(contextZone)
                .toLocalDate();
            return java.sql.Date.valueOf(localDate);
        }
    }

    /**
     * Timestamp to Time (extract time part).
     */
    public java.sql.Time timestampToTime(DingoDateTime timestamp, ZoneId contextZone) {
        if (timestamp == null) {
            return null;
        }

        if (!timestamp.isTimeZoneSensitive()) {
            if (timestamp instanceof DingoDateTime.DingoLocalDateTime) {
                DingoDateTime.DingoLocalDateTime localDt = (DingoDateTime.DingoLocalDateTime) timestamp;
                LocalTime localTime = localDt.getValue().toLocalTime();
                return java.sql.Time.valueOf(localTime);
            } else if (timestamp instanceof DingoDateTime.DingoTimestampTZ) {
                DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) timestamp;
                LocalTime localTime = tzValue.getUtcValue()
                    .atZone(tzValue.getOriginalZone())
                    .toLocalTime();
                return java.sql.Time.valueOf(localTime);
            } else {
                throw new IllegalArgumentException("Unsupported timestamp type for time conversion: "
                                                   + timestamp.getClass());
            }
        } else {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) timestamp;
            LocalTime localTime = tzValue.getUtcValue()
                .atZone(contextZone)
                .toLocalTime();
            return java.sql.Time.valueOf(localTime);
        }
    }

    /**
     * Timestamp to Time (extract time part).
     */
    public java.sql.Timestamp timestampTZToTimestamp(DingoDateTime timestamp, ZoneId contextZone) {
        if (timestamp == null) {
            return null;
        }

        if (!timestamp.isTimeZoneSensitive()) {
            if (timestamp instanceof DingoDateTime.DingoLocalDateTime) {
                DingoDateTime.DingoLocalDateTime localDt = (DingoDateTime.DingoLocalDateTime) timestamp;
                LocalDateTime localDateTime = localDt.getValue();
                if (localDateTime.getYear() < 0) {
                    return null;
                }
                return java.sql.Timestamp.valueOf(localDateTime);
            } else if (timestamp instanceof DingoDateTime.DingoTimestampTZ) {
                DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) timestamp;
                LocalDateTime localDateTime = tzValue.getUtcValue()
                    .atZone(tzValue.getOriginalZone())
                    .toLocalDateTime();
                if (localDateTime.getYear() < 0) {
                    return null;
                }
                return java.sql.Timestamp.valueOf(localDateTime);
            } else {
                throw new IllegalArgumentException("Unsupported timestamp type for time conversion: "
                                                   + timestamp.getClass());
            }
        } else {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) timestamp;
            LocalDateTime localDateTime = tzValue.getUtcValue()
                .atZone(contextZone)
                .toLocalDateTime();
            return java.sql.Timestamp.valueOf(localDateTime);
        }
    }

    /**
     * Convert Date to Timestamp (complement the time part with 00:00:00).
     */
    public java.sql.Timestamp dateToTimestamp(DingoDateTime date, ZoneId contextZone) {
        if (date == null) {
            return null;
        }

        if (!date.isTimeZoneSensitive()) {
            if (date instanceof DingoDateTime.DingoLocalDate) {
                DingoDateTime.DingoLocalDate localDate = (DingoDateTime.DingoLocalDate) date;
                LocalDateTime localDateTime = localDate.getValue().atStartOfDay();
                if (localDateTime.getYear() < 0) {
                    return null;
                }
                return java.sql.Timestamp.valueOf(localDateTime);
            } else {
                throw new IllegalArgumentException("Unsupported date type for timestamp conversion: "
                                                   + date.getClass());
            }
        } else {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) date;
            LocalDateTime startOfDay = tzValue.getUtcValue()
                .atZone(tzValue.getOriginalZone())
                .toLocalDate()
                .atStartOfDay();
            ZonedDateTime zdt = startOfDay.atZone(tzValue.getOriginalZone());
            return java.sql.Timestamp.from(zdt.toInstant());
        }
    }

    public java.sql.Timestamp timeToTimestamp(DingoDateTime time, ZoneId contextZone) {
        if (time == null) {
            return null;
        }

        if (!time.isTimeZoneSensitive()) {
            if (time instanceof DingoDateTime.DingoLocalTime) {
                DingoDateTime.DingoLocalTime localTime = (DingoDateTime.DingoLocalTime) time;
                LocalDateTime localDateTime = LocalDate.of(1970, 1, 1).atTime(localTime.getValue());
                return java.sql.Timestamp.valueOf(localDateTime);
            } else {
                throw new IllegalArgumentException("Unsupported time type for timestamp conversion: "
                                                   + time.getClass());
            }
        } else {
            throw new IllegalArgumentException(
                "Converting time with time zone to timestamp is not supported");
        }
    }

    public java.sql.Timestamp timeToTimestampWithRef(DingoDateTime time, LocalDate referenceDate, ZoneId contextZone) {
        if (time == null) {
            return null;
        }

        if (!time.isTimeZoneSensitive()) {
            DingoDateTime.DingoLocalTime localTime = (DingoDateTime.DingoLocalTime) time;
            LocalDateTime localDateTime = LocalDateTime.of(referenceDate, localTime.getValue());
            return java.sql.Timestamp.valueOf(localDateTime);
        } else {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) time;
            LocalTime localTime = tzValue.getUtcValue()
                .atZone(tzValue.getOriginalZone())
                .toLocalTime();
            LocalDateTime localDateTime = LocalDateTime.of(referenceDate, localTime);
            ZonedDateTime zdt = localDateTime.atZone(tzValue.getOriginalZone());
            return java.sql.Timestamp.from(zdt.toInstant());
        }
    }

    public Object convert(DingoDateTime source, DateTimeType sourceType, DateTimeType targetType, ZoneId contextZone) {
        if (source == null) {
            return null;
        }

        if (sourceType == DateTimeType.TIMESTAMP || sourceType == DateTimeType.TIMESTAMP_TZ) {
            if (targetType == DateTimeType.DATE) {
                return timestampToDate(source, contextZone);
            } else if (targetType == DateTimeType.TIME) {
                return timestampToTime(source, contextZone);
            } else if (targetType == DateTimeType.TIMESTAMP) {
                return timestampTZToTimestamp(source, contextZone);
            }
        } else if (sourceType == DateTimeType.DATE) {
            if (targetType == DateTimeType.TIMESTAMP || targetType == DateTimeType.TIMESTAMP_TZ) {
                return dateToTimestamp(source, contextZone);
            } else if (targetType == DateTimeType.DATE) {
                LocalDate localDate = (LocalDate) source.getValue();
                if (localDate.getYear() < 0) {
                    return null;
                }
                return Date.valueOf(localDate);
            }
        } else if (sourceType == DateTimeType.TIME) {
            if (targetType == DateTimeType.TIMESTAMP || targetType == DateTimeType.TIMESTAMP_TZ) {
                return timeToTimestamp(source, contextZone);
                // return null;
            } else if (targetType == DateTimeType.TIME) {
                LocalTime localTime = (LocalTime) source.getValue();
                Calendar instance = Calendar.getInstance();
                instance.setTimeInMillis(localTime.toSecondOfDay() * 1000 + localTime.getNano() / 1000000);
                instance.setTimeZone(TimeZone.getTimeZone(contextZone));
                long v = instance.getTimeInMillis();
                return new Time(v - instance.getTimeZone().getOffset(v));
            } else if (targetType == DateTimeType.DATE) {
                return null;
            }
        }

        throw new IllegalArgumentException("Unsupported conversion: " + sourceType + " to " + targetType);
    }
}
