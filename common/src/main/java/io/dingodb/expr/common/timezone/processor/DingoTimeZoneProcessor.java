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

import io.dingodb.expr.common.timezone.DateTimeUtils;
import io.dingodb.expr.common.timezone.converter.SafeOutputFormatter;
import io.dingodb.expr.common.timezone.core.DateTimeProcessingException;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.core.SimpleTimeZoneConfig;
import io.dingodb.expr.common.timezone.operations.ExtractionOperations;
import io.dingodb.expr.common.timezone.operations.FormattingOperations;
import io.dingodb.expr.common.timezone.operations.OperationResult;
import lombok.Getter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class DingoTimeZoneProcessor {

    @Getter
    private final ThreeTierTimeZoneProcessor tierProcessor;
    private final SafeOutputFormatter outputFormatter;

    public DingoTimeZoneProcessor() {
        this(SimpleTimeZoneConfig.getApplicationZone(),
            SimpleTimeZoneConfig.getStorageZone(),
            SimpleTimeZoneConfig.getApplicationZone());
    }

    public DingoTimeZoneProcessor(ZoneId applicationZone) {
        this(applicationZone, ZoneId.of("UTC"), applicationZone);
    }

    public DingoTimeZoneProcessor(ZoneId inputZone, ZoneId storageZone, ZoneId outputZone) {
        this.tierProcessor = new ThreeTierTimeZoneProcessor(inputZone, storageZone, outputZone);
        this.outputFormatter = new SafeOutputFormatter();
    }

    public Object processDateTime(Object input, DateTimeType inputType,
                                  DateTimeType outputType) {
        if (input == null || inputType == null) {
            return null;
        }
        // Fast path: same type, non-TZ-sensitive SQL types — no conversion needed
        if (inputType == outputType) {
            if (input instanceof java.sql.Date && inputType == DateTimeType.DATE) {
                return input;
            }
            if (input instanceof java.sql.Time && inputType == DateTimeType.TIME) {
                return input;
            }
            if (input instanceof java.sql.Timestamp && inputType == DateTimeType.TIMESTAMP) {
                return input;
            }
        }
        try {
            DingoDateTime internal = tierProcessor.convertInput(input, inputType);

            DingoDateTime processed = processBusinessLogic(internal);

            if (processed == null) {
                return null;
            }
            return tierProcessor.convertOutput(processed, outputType);

        } catch (Exception e) {
            /*throw new DateTimeProcessingException(
                "Failed to process datetime input: " + input, e);*/
            return null;
        }
    }

    public Object processDateTime(Object input, DateTimeType outputType) {
        DateTimeType inputType = inferInputType(input);
        return processDateTime(input, inputType, outputType);
    }

    public String toSafeString(Object dateTimeValue) {
        return outputFormatter.toSafeString(dateTimeValue,
            tierProcessor.getOutputZone());
    }

    public String formatDingoDateTime(DingoDateTime dingoDateTime) {
        return outputFormatter.formatDingoDateTime(dingoDateTime,
            tierProcessor.getOutputZone());
    }

    private DingoDateTime processBusinessLogic(DingoDateTime internal) {
        // TODO
        return internal; // By default, it returns directly. In actual use, calculation logic can be added.
    }

    public DateTimeType inferInputType(Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof java.sql.Date) {
            return DateTimeType.DATE;
        } else if (input instanceof java.sql.Time) {
            return DateTimeType.TIME;
        } else if (input instanceof java.sql.Timestamp || input instanceof java.util.Date) {
            return DateTimeType.TIMESTAMP;
        } else if (input instanceof String) {
            /*InputTypeAnalyzer.TimeZoneSensitivity sensitivity =
                InputTypeAnalyzer.analyzeSensitivity(input, DateTimeType.TIMESTAMP,
                    tierProcessor.getInputZone());
            return sensitivity == InputTypeAnalyzer.TimeZoneSensitivity.SENSITIVE ?
                DateTimeType.TIMESTAMP_TZ : DateTimeType.TIMESTAMP;*/
            return DateTimeUtils.analyzeTemporalType((String) input);
        } else if (input instanceof DingoDateTime) {
            return ((DingoDateTime) input).getType();
        }

        return DateTimeType.TIMESTAMP;
    }

    public ZoneId getInputZone() {
        return tierProcessor.getInputZone();
    }

    public ZoneId getOutputZone() {
        return tierProcessor.getOutputZone();
    }

    public ZoneId getStorageZone() {
        return tierProcessor.getStorageZone();
    }

    public DingoDateTime dateAdd(DingoDateTime dateTime, long amount, ChronoUnit unit) {
        if (dateTime.isTimeZoneSensitive()) {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dateTime;
            ZonedDateTime zdt = tzValue.getUtcValue().atZone(tzValue.getOriginalZone());
            return new DingoDateTime.DingoTimestampTZ(zdt.plus(amount, unit).toInstant(), tzValue.getOriginalZone());
        }
        Object value = dateTime.getValue();
        if (value instanceof LocalDate) {
            return new DingoDateTime.DingoLocalDate(((LocalDate) value).plus(amount, unit));
        } else if (value instanceof LocalDateTime) {
            return new DingoDateTime.DingoLocalDateTime(((LocalDateTime) value).plus(amount, unit));
        } else if (value instanceof LocalTime) {
            return new DingoDateTime.DingoLocalTime(((LocalTime) value).plus(amount, unit));
        }
        throw new DateTimeProcessingException("Unsupported type for add: " + value.getClass());
    }

    public DingoDateTime dateSubtract(DingoDateTime dateTime, long amount, ChronoUnit unit) {
        if (dateTime.isTimeZoneSensitive()) {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dateTime;
            ZonedDateTime zdt = tzValue.getUtcValue().atZone(tzValue.getOriginalZone());
            return new DingoDateTime.DingoTimestampTZ(zdt.minus(amount, unit).toInstant(), tzValue.getOriginalZone());
        }
        Object value = dateTime.getValue();
        if (value instanceof LocalDate) {
            return new DingoDateTime.DingoLocalDate(((LocalDate) value).minus(amount, unit));
        } else if (value instanceof LocalDateTime) {
            return new DingoDateTime.DingoLocalDateTime(((LocalDateTime) value).minus(amount, unit));
        } else if (value instanceof LocalTime) {
            return new DingoDateTime.DingoLocalTime(((LocalTime) value).minus(amount, unit));
        }
        throw new DateTimeProcessingException("Unsupported type for subtract: " + value.getClass());
    }

    // -------------------------------------------------------------------------
    // Type-specialized extraction fast paths — bypass the full pipeline
    // for non-timezone-sensitive java.sql types (Date, Timestamp).
    // -------------------------------------------------------------------------

    public Integer extractYear(java.sql.Date value) {
        return value == null ? null : value.toLocalDate().getYear();
    }

    public Integer extractYear(java.sql.Timestamp value) {
        return value == null ? null : value.toLocalDateTime().getYear();
    }

    public Integer extractYear(Object input) {
        ExtractionOperations.ExtractFieldOperation operation = ExtractionOperations.year();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null) {
            return null;
        }
        if (dateTime instanceof DingoDateTime.DingoLocalTime) {
            return null;
        }
        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }
        throw new DateTimeProcessingException("Extract year operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public Integer extractMonth(java.sql.Date value) {
        return value == null ? null : value.toLocalDate().getMonthValue();
    }

    public Integer extractMonth(java.sql.Timestamp value) {
        return value == null ? null : value.toLocalDateTime().getMonthValue();
    }

    public Integer extractMonth(Object input) {
        ExtractionOperations.ExtractFieldOperation operation = ExtractionOperations.month();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null) {
            return null;
        }
        if (dateTime instanceof DingoDateTime.DingoLocalTime) {
            return null;
        }

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }
        throw new DateTimeProcessingException("Extract month operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public Integer extractDay(java.sql.Date value) {
        return value == null ? null : value.toLocalDate().getDayOfMonth();
    }

    public Integer extractDay(java.sql.Timestamp value) {
        return value == null ? null : value.toLocalDateTime().getDayOfMonth();
    }

    public Integer extractDay(Object input) {
        ExtractionOperations.ExtractFieldOperation operation = ExtractionOperations.dayOfMonth();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null) {
            return null;
        }
        if (dateTime instanceof DingoDateTime.DingoLocalTime) {
            return null;
        }

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }
        throw new DateTimeProcessingException("Extract day operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public Integer extractQuarter(java.sql.Date value) {
        return value == null ? null : (value.toLocalDate().getMonthValue() - 1) / 3 + 1;
    }

    public Integer extractQuarter(java.sql.Timestamp value) {
        return value == null ? null : (value.toLocalDateTime().getMonthValue() - 1) / 3 + 1;
    }

    public Integer extractQuarter(Object input) {
        ExtractionOperations.ExtractQuarterOperation operation = ExtractionOperations.quarter();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null) {
            return null;
        }
        if (dateTime instanceof DingoDateTime.DingoLocalTime) {
            return null;
        }

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }
        throw new DateTimeProcessingException("Extract quarter operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public Integer extractWeek(java.sql.Date value) {
        if (value == null) {
            return null;
        }
        return value.toLocalDate().get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    }

    public Integer extractWeek(java.sql.Timestamp value) {
        if (value == null) {
            return null;
        }
        return value.toLocalDateTime().get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    }

    public Integer extractWeek(Object input) {
        ExtractionOperations.ExtractFieldOperation operation = ExtractionOperations.week();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null) {
            return null;
        }
        if (dateTime instanceof DingoDateTime.DingoLocalTime) {
            return null;
        }

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }

        throw new DateTimeProcessingException("Extract week operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public Integer extractHour(java.sql.Date value) {
        return 0;
    }

    public Integer extractHour(java.sql.Timestamp value) {
        return value == null ? 0 : value.toLocalDateTime().getHour();
    }

    public Integer extractHour(java.sql.Time value) {
        return value == null ? 0 : value.toLocalTime().getHour();
    }

    public Integer extractHour(Object input) {
        ExtractionOperations.ExtractFieldOperation operation = ExtractionOperations.hour();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null || dateTime instanceof DingoDateTime.DingoLocalDate) {
            return 0;
        }

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }
        throw new DateTimeProcessingException("Extract hour operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public Integer extractMinute(java.sql.Date value) {
        return 0;
    }

    public Integer extractMinute(java.sql.Timestamp value) {
        return value == null ? 0 : value.toLocalDateTime().getMinute();
    }

    public Integer extractMinute(java.sql.Time value) {
        return value == null ? 0 : value.toLocalTime().getMinute();
    }

    public Integer extractMinute(Object input) {
        ExtractionOperations.ExtractFieldOperation operation = ExtractionOperations.minute();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null || dateTime instanceof DingoDateTime.DingoLocalDate) {
            return 0;
        }

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }
        throw new DateTimeProcessingException("Extract minute operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public Integer extractSecond(java.sql.Date value) {
        return 0;
    }

    public Integer extractSecond(java.sql.Timestamp value) {
        return value == null ? 0 : value.toLocalDateTime().getSecond();
    }

    public Integer extractSecond(java.sql.Time value) {
        return value == null ? 0 : value.toLocalTime().getSecond();
    }

    public Integer extractSecond(Object input) {
        ExtractionOperations.ExtractFieldOperation operation = ExtractionOperations.second();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null || dateTime instanceof DingoDateTime.DingoLocalDate) {
            return 0;
        }

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }
        throw new DateTimeProcessingException("Extract second operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public Integer extractMillisecond(java.sql.Date value) {
        return 0;
    }
    
    public Integer extractMillisecond(java.sql.Timestamp value) {
        if (value == null) {
            return 0;
        }
        return value.toLocalDateTime().getNano() / 1_000_000;
    }

    public Integer extractMillisecond(Object input) {
        ExtractionOperations.ExtractFieldOperation operation = ExtractionOperations.millisecond();
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);
        if (dateTime == null || dateTime instanceof DingoDateTime.DingoLocalDate) {
            return 0;
        }

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof Integer) {
            return (Integer) result.getValue();
        }
        throw new DateTimeProcessingException("Extract millisecond operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public DingoDateTime currentTimestamp() {
        Instant now = Instant.now();
        return new DingoDateTime.DingoTimestampTZ(now, getOutputZone());
    }

    public DingoDateTime currentDate() {
        Instant now = Instant.now();
        LocalDate localDate = now.atZone(getOutputZone()).toLocalDate();
        return new DingoDateTime.DingoLocalDate(localDate);
    }

    public DingoDateTime currentTime() {
        Instant now = Instant.now();
        java.time.LocalTime localTime = now.atZone(getOutputZone()).toLocalTime();
        return new DingoDateTime.DingoLocalTime(localTime);
    }

    public int compare(Object left, Object right) {
        DateTimeType leftInputType = inferInputType(left);
        DateTimeType rightInputType = inferInputType(right);
        DingoDateTime leftDateTime = tierProcessor.convertInput(left, leftInputType);
        DingoDateTime rightDateTime = tierProcessor.convertInput(right, rightInputType);
        DingoDateTime normalizedLeft = normalizeForComparison(leftDateTime);
        DingoDateTime normalizedRight = normalizeForComparison(rightDateTime);

        @SuppressWarnings("unchecked")
        Comparable<Object> leftComparable = (Comparable<Object>) normalizedLeft.getValue();
        @SuppressWarnings("unchecked")
        Comparable<Object> rightComparable = (Comparable<Object>) normalizedRight.getValue();

        return leftComparable.compareTo(rightComparable);
    }

    public String formatDateTime(Object input, DateTimeFormatter formatter) {
        FormattingOperations.FormatOperation operation = new FormattingOperations.FormatOperation(formatter);
        DateTimeType inputType = inferInputType(input);
        DingoDateTime dateTime = tierProcessor.convertInput(input, inputType);

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof String) {
            return (String) result.getValue();
        }

        throw new DateTimeProcessingException("Format date time operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public boolean isBefore(Object dateTime1, Object dateTime2) {
        return compare(dateTime1, dateTime2) < 0;
    }

    public boolean isAfter(Object dateTime1, Object dateTime2) {
        return compare(dateTime1, dateTime2) > 0;
    }

    public boolean isEqual(Object dateTime1, Object dateTime2) {
        return compare(dateTime1, dateTime2) == 0;
    }

    private static DingoDateTime normalizeForComparison(DingoDateTime dateTime) {
        if (dateTime.isTimeZoneSensitive()) {
            DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dateTime;
            return new DingoDateTime.DingoTimestampTZ(tzValue.getUtcValue(), java.time.ZoneId.of("UTC"));
        }
        return dateTime;
    }

}

