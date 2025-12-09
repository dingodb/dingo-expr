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
import io.dingodb.expr.common.timezone.operations.ArithmeticOperations;
import io.dingodb.expr.common.timezone.operations.ExtractionOperations;
import io.dingodb.expr.common.timezone.operations.FormattingOperations;
import io.dingodb.expr.common.timezone.operations.OperationResult;
import lombok.Getter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
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
        ArithmeticOperations.AddOperation operation = new ArithmeticOperations.AddOperation(unit, amount);

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof DingoDateTime) {
            return (DingoDateTime) result.getValue();
        }

        throw new DateTimeProcessingException("Date add operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
    }

    public DingoDateTime dateSubtract(DingoDateTime dateTime, long amount, ChronoUnit unit) {
        ArithmeticOperations.SubtractOperation operation = new ArithmeticOperations.SubtractOperation(unit, amount);

        OperationResult result = (OperationResult) operation.execute(new DingoDateTime[]{dateTime}, getOutputZone());

        if (result.isSuccess() && result.getValue() instanceof DingoDateTime) {
            return (DingoDateTime) result.getValue();
        }

        throw new DateTimeProcessingException("Date subtract operation failed: "
              + (result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error"));
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

