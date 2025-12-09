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

import io.dingodb.expr.common.timezone.converter.DateTimeConverter;
import io.dingodb.expr.common.timezone.converter.SafeTypeConverter;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.core.SimpleTimeZoneConfig;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * Three-layer time zone conversion architecture: input time zone → storage time zone (UTC) → output time zone.
 */
public class ThreeTierTimeZoneProcessor {

    private final ZoneId inputZone;
    private final ZoneId storageZone;
    private final ZoneId outputZone;
    private final DateTimeConverter inputConverter;
    private final SafeTypeConverter outputConverter;

    public ThreeTierTimeZoneProcessor(ZoneId inputZone, ZoneId storageZone, ZoneId outputZone) {
        this.inputZone = inputZone;
        this.storageZone = storageZone;
        this.outputZone = outputZone;
        this.inputConverter = new DateTimeConverter(inputZone);
        this.outputConverter = new SafeTypeConverter(outputZone);
    }

    public ThreeTierTimeZoneProcessor(ZoneId applicationZone) {
        this(applicationZone, ZoneId.of("UTC"), applicationZone);
    }

    public ThreeTierTimeZoneProcessor() {
        this(SimpleTimeZoneConfig.getApplicationZone());
    }

    /**
     * Input conversion: Convert to internal storage format.
     */
    public DingoDateTime convertInput(Object input, DateTimeType targetType) {
        DingoDateTime internal = inputConverter.convertInput(input, targetType);

        if (internal == null) {
            return null;
        }
        return toStorageFormat(internal);
    }

    public Object convertOutput(DingoDateTime internal, DateTimeType outputType) {
        DingoDateTime outputReady = fromStorageFormat(internal);

        return outputConverter.convert(outputReady, outputReady.getType(), outputType, outputZone);
    }

    private DingoDateTime toStorageFormat(DingoDateTime internal) {
        if (!internal.isTimeZoneSensitive()) {
            return internal;
        }

        DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) internal;
        if (tzValue.getOriginalZone().equals(storageZone) || tzValue.equals("Z")) {
            return internal;
        }

        return new DingoDateTime.DingoTimestampTZ(tzValue.getUtcValue(), storageZone);
    }

    private DingoDateTime fromStorageFormat(DingoDateTime internal) {
        if (!internal.isTimeZoneSensitive()) {
            return internal;
        }

        DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) internal;
        return new DingoDateTime.DingoTimestampTZ(tzValue.getUtcValue(), outputZone);
    }

    private Object toOutputType(DingoDateTime outputReady, DateTimeType outputType) {
        switch (outputType) {
            case DATE:
                if (outputReady.isTimeZoneSensitive()) {
                    DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) outputReady;
                    LocalDate localDate = tzValue.getUtcValue()
                        .atZone(outputZone)
                        .toLocalDate();
                    return java.sql.Date.valueOf(localDate);
                } else {
                    Object value = outputReady.getValue();
                    LocalDate localDate;

                    if (value instanceof LocalDateTime) {
                        localDate = ((LocalDateTime) value).toLocalDate();
                    } else if (value instanceof LocalDate) {
                        localDate = (LocalDate) value;
                    } else {
                        throw new IllegalArgumentException("Unsupported internal type for DATE conversion: "
                                                           + value.getClass());
                    }
                    return java.sql.Date.valueOf(localDate);
                }

            case TIME:
                if (outputReady.isTimeZoneSensitive()) {
                    DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) outputReady;
                    LocalTime localTime = tzValue.getUtcValue()
                        .atZone(outputZone)
                        .toLocalTime();
                    return java.sql.Time.valueOf(localTime);
                } else {
                    DingoDateTime.DingoLocalTime localTime = (DingoDateTime.DingoLocalTime) outputReady;
                    return java.sql.Time.valueOf(localTime.getValue());
                }

            case TIMESTAMP:
                if (outputReady.isTimeZoneSensitive()) {
                    DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) outputReady;
                    LocalDateTime localDateTime = tzValue.getUtcValue()
                        .atZone(outputZone)
                        .toLocalDateTime();
                    return java.sql.Timestamp.valueOf(localDateTime);
                } else {
                    Object value = outputReady.getValue();
                    LocalDateTime localDateTime;

                    if (value instanceof LocalDateTime) {
                        localDateTime = (LocalDateTime) value;
                    } else if (value instanceof LocalDate) {
                        localDateTime = ((LocalDate) value).atStartOfDay();
                    } else if (value instanceof LocalTime) {
                        localDateTime = LocalDate.of(1970, 1, 1).atTime((LocalTime) value);
                    } else {
                        throw new IllegalArgumentException("Unsupported internal type for TIMESTAMP conversion: "
                                                           + value.getClass());
                    }

                    return java.sql.Timestamp.valueOf(localDateTime);
                }

            case TIMESTAMP_TZ:
                return outputReady.getValue();

            default:
                throw new IllegalArgumentException("Unsupported output type: " + outputType);
        }
    }

    public ZoneId getInputZone() {
        return inputZone;
    }

    public ZoneId getStorageZone() {
        return storageZone;
    }

    public ZoneId getOutputZone() {
        return outputZone;
    }
}
