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

package io.dingodb.expr.common.timezone.operations;

import io.dingodb.expr.common.timezone.core.DingoDateTime;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;

public class ExtractionOperations {

    public static class ExtractFieldOperation implements DateTimeOperation {
        private final TemporalField field;
        private final String fieldName;

        public ExtractFieldOperation(TemporalField field, String fieldName) {
            this.field = field;
            this.fieldName = fieldName;
        }

        @Override
        public Object execute(DingoDateTime[] operands, ZoneId contextZone) {
            validateOperands(operands);

            try {
                DingoDateTime dateTime = operands[0];

                if (dateTime.isTimeZoneSensitive()) {
                    DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dateTime;
                    ZonedDateTime zdt = tzValue.getUtcValue().atZone(tzValue.getOriginalZone());
                    int value = zdt.get(field);
                    return OperationResult.success(value);
                } else {
                    Object value = dateTime.getValue();
                    int result;

                    if (value instanceof java.time.LocalDateTime) {
                        result = ((java.time.LocalDateTime) value).get(field);
                    } else if (value instanceof java.time.LocalDate) {
                        result = ((java.time.LocalDate) value).get(field);
                    } else if (value instanceof java.time.LocalTime) {
                        result = ((java.time.LocalTime) value).get(field);
                    } else {
                        return OperationResult.failure("Unsupported type for extract operation: " + value.getClass());
                    }
                    return OperationResult.success(result);
                }
            } catch (Exception e) {
                return OperationResult.failure("Extract operation failed: " + e.getMessage());
            }
        }

        @Override
        public String getName() {
            return "EXTRACT_" + fieldName;
        }

        @Override
        public String getDescription() {
            return "Extract " + fieldName + " from date time";
        }

        @Override
        public void validateOperands(DingoDateTime[] operands) {
            if (operands == null || operands.length != 1) {
                throw new IllegalArgumentException("Extract operation requires exactly 1 operand");
            }
        }
    }

    public static class ExtractQuarterOperation implements DateTimeOperation {
        @Override
        public Object execute(DingoDateTime[] operands, ZoneId contextZone) {
            validateOperands(operands);

            try {
                DingoDateTime dateTime = operands[0];

                if (dateTime.isTimeZoneSensitive()) {
                    DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dateTime;
                    ZonedDateTime zdt = tzValue.getUtcValue().atZone(tzValue.getOriginalZone());
                    int quarter = getQuarter(zdt.getMonthValue());
                    return OperationResult.success(quarter);
                } else {
                    Object value = dateTime.getValue();
                    int month;

                    if (value instanceof java.time.LocalDateTime) {
                        month = ((java.time.LocalDateTime) value).getMonthValue();
                    } else if (value instanceof java.time.LocalDate) {
                        month = ((java.time.LocalDate) value).getMonthValue();
                    } else {
                        return OperationResult.failure("Cannot extract quarter from type: " + value.getClass());
                    }

                    int quarter = getQuarter(month);
                    return OperationResult.success(quarter);
                }
            } catch (Exception e) {
                return OperationResult.failure("Extract quarter operation failed: " + e.getMessage());
            }
        }

        @Override
        public String getName() {
            return "EXTRACT_QUARTER";
        }

        @Override
        public String getDescription() {
            return "Extract quarter from date time";
        }

        @Override
        public void validateOperands(DingoDateTime[] operands) {
            if (operands == null || operands.length != 1) {
                throw new IllegalArgumentException("Extract quarter operation requires exactly 1 operand");
            }
        }

        private int getQuarter(int month) {
            return (month - 1) / 3 + 1;
        }
    }

    public static ExtractFieldOperation year() {
        return new ExtractFieldOperation(ChronoField.YEAR, "YEAR");
    }

    public static ExtractFieldOperation month() {
        return new ExtractFieldOperation(ChronoField.MONTH_OF_YEAR, "MONTH");
    }

    public static ExtractFieldOperation dayOfMonth() {
        return new ExtractFieldOperation(ChronoField.DAY_OF_MONTH, "DAY");
    }

    public static ExtractFieldOperation hour() {
        return new ExtractFieldOperation(ChronoField.HOUR_OF_DAY, "HOUR");
    }

    public static ExtractFieldOperation minute() {
        return new ExtractFieldOperation(ChronoField.MINUTE_OF_HOUR, "MINUTE");
    }

    public static ExtractFieldOperation second() {
        return new ExtractFieldOperation(ChronoField.SECOND_OF_MINUTE, "SECOND");
    }

    public static ExtractFieldOperation millisecond() {
        return new ExtractFieldOperation(ChronoField.MILLI_OF_SECOND, "millisecond");
    }

    public static ExtractFieldOperation week() {
        return new ExtractFieldOperation(IsoFields.WEEK_OF_WEEK_BASED_YEAR, "WEEK");
    }

    public static ExtractQuarterOperation quarter() {
        return new ExtractQuarterOperation();
    }

}
