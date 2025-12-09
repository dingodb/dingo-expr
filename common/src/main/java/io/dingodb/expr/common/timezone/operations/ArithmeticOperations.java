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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class ArithmeticOperations {

    public static class AddOperation implements DateTimeOperation {
        private final ChronoUnit unit;
        private final long amount;

        public AddOperation(ChronoUnit unit, long amount) {
            this.unit = unit;
            this.amount = amount;
        }

        @Override
        public Object execute(DingoDateTime[] operands, ZoneId contextZone) {
            validateOperands(operands);

            try {
                DingoDateTime dateTime = operands[0];

                if (dateTime.isTimeZoneSensitive()) {
                    DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dateTime;
                    ZonedDateTime zdt = tzValue.getUtcValue().atZone(tzValue.getOriginalZone());
                    ZonedDateTime resultZdt = zdt.plus(amount, unit);
                    return OperationResult.success(
                        new DingoDateTime.DingoTimestampTZ(resultZdt.toInstant(), tzValue.getOriginalZone())
                    );
                } else {
                    Object value = dateTime.getValue();
                    Object result;

                    if (value instanceof LocalDateTime) {
                        result = ((LocalDateTime) value).plus(amount, unit);
                        return OperationResult.success(new DingoDateTime.DingoLocalDateTime((LocalDateTime) result));
                    } else if (value instanceof LocalDate) {
                        result = ((LocalDate) value).plus(amount, unit);
                        return OperationResult.success(new DingoDateTime.DingoLocalDate((LocalDate) result));
                    } else if (value instanceof LocalTime) {
                        result = ((LocalTime) value).plus(amount, unit);
                        return OperationResult.success(new DingoDateTime.DingoLocalTime((LocalTime) result));
                    } else {
                        return OperationResult.failure("Unsupported type for add operation: " + value.getClass());
                    }
                }
            } catch (Exception e) {
                return OperationResult.failure("Add operation failed: " + e.getMessage());
            }
        }

        @Override
        public String getName() {
            return "ADD_" + unit.name();
        }

        @Override
        public String getDescription() {
            return "Add " + amount + " " + unit + " to date time";
        }

        @Override
        public void validateOperands(DingoDateTime[] operands) {
            if (operands == null || operands.length != 1) {
                throw new IllegalArgumentException("Add operation requires exactly 1 operand");
            }
        }
    }

    public static class SubtractOperation implements DateTimeOperation {
        private final ChronoUnit unit;
        private final long amount;

        public SubtractOperation(ChronoUnit unit, long amount) {
            this.unit = unit;
            this.amount = amount;
        }

        @Override
        public Object execute(DingoDateTime[] operands, ZoneId contextZone) {
            validateOperands(operands);

            try {
                DingoDateTime dateTime = operands[0];

                if (dateTime.isTimeZoneSensitive()) {
                    DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dateTime;
                    ZonedDateTime zdt = tzValue.getUtcValue().atZone(tzValue.getOriginalZone());
                    ZonedDateTime resultZdt = zdt.minus(amount, unit);
                    return OperationResult.success(
                        new DingoDateTime.DingoTimestampTZ(resultZdt.toInstant(), tzValue.getOriginalZone())
                    );
                } else {
                    Object value = dateTime.getValue();
                    Object result;

                    if (value instanceof LocalDateTime) {
                        result = ((LocalDateTime) value).minus(amount, unit);
                        return OperationResult.success(new DingoDateTime.DingoLocalDateTime((LocalDateTime) result));
                    } else if (value instanceof LocalDate) {
                        result = ((LocalDate) value).minus(amount, unit);
                        return OperationResult.success(new DingoDateTime.DingoLocalDate((LocalDate) result));
                    } else if (value instanceof LocalTime) {
                        result = ((LocalTime) value).minus(amount, unit);
                        return OperationResult.success(new DingoDateTime.DingoLocalTime((LocalTime) result));
                    } else {
                        return OperationResult.failure("Unsupported type for subtract operation: " + value.getClass());
                    }
                }
            } catch (Exception e) {
                return OperationResult.failure("Subtract operation failed: " + e.getMessage());
            }
        }

        @Override
        public String getName() {
            return "SUBTRACT_" + unit.name();
        }

        @Override
        public String getDescription() {
            return "Subtract " + amount + " " + unit + " from date time";
        }

        @Override
        public void validateOperands(DingoDateTime[] operands) {
            if (operands == null || operands.length != 1) {
                throw new IllegalArgumentException("Subtract operation requires exactly 1 operand");
            }
        }
    }
}
