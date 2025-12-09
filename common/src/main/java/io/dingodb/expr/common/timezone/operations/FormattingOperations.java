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
import java.time.format.DateTimeFormatter;

public class FormattingOperations {

    public static class FormatOperation implements DateTimeOperation {
        private final DateTimeFormatter formatter;

        public FormatOperation(DateTimeFormatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public Object execute(DingoDateTime[] operands, ZoneId contextZone) {
            try {
                DingoDateTime dateTime = operands[0];

                if (dateTime.isTimeZoneSensitive()) {
                    DingoDateTime.DingoTimestampTZ tzValue = (DingoDateTime.DingoTimestampTZ) dateTime;
                    ZonedDateTime zdt = tzValue.getUtcValue().atZone(tzValue.getOriginalZone());
                    String formatted = zdt.format(formatter);
                    return OperationResult.success(formatted);
                } else {
                    Object value = dateTime.getValue();
                    String formatted;

                    if (value instanceof java.time.LocalDateTime) {
                        formatted = ((java.time.LocalDateTime) value).format(formatter);
                    } else if (value instanceof java.time.LocalDate) {
                        formatted = ((java.time.LocalDate) value).format(formatter);
                    } else if (value instanceof java.time.LocalTime) {
                        formatted = ((java.time.LocalTime) value).format(formatter);
                    } else {
                        return OperationResult.failure("Unsupported type for format operation: " + value.getClass());
                    }

                    return OperationResult.success(formatted);
                }
            } catch (Exception e) {
                return OperationResult.failure("Format operation failed: " + e.getMessage());
            }
        }

        @Override
        public String getName() {
            return "FORMAT";
        }

        @Override
        public String getDescription() {
            return "Format date time with pattern: " + formatter.toString();
        }
    }
}
