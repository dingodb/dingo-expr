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

package io.dingodb.expr.common.timezone;

import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;

import java.time.ZoneId;

public class InputTypeAnalyzer {

    public enum TimeZoneSensitivity {
        SENSITIVE,      // Explicit time zone sensitivity
        INSENSITIVE,    // Explicit time zone insensitivity
        AMBIGUOUS,      // Fuzzy, requires contextual judgment
        UNKNOWN         // unknown
    }

    public static TimeZoneSensitivity analyzeSensitivity(Object input, DateTimeType targetType, ZoneId contextZone) {
        if (input == null) {
            return TimeZoneSensitivity.UNKNOWN;
        }

        if (targetType.isTimeZoneSensitive()) {
            return analyzeForTimeZoneSensitiveTarget(input, contextZone);
        } else {
            return analyzeForTimeZoneInsensitiveTarget(input, contextZone);
        }
    }

    private static TimeZoneSensitivity analyzeForTimeZoneSensitiveTarget(Object input, ZoneId contextZone) {
        if (input instanceof String) {
            return analyzeStringInput((String) input);
        } else if (input instanceof java.sql.Timestamp) {
            return TimeZoneSensitivity.SENSITIVE;
        } else if (input instanceof java.sql.Date) {
            // sql.Date is designed to be time zone insensitive, but actually stores timestamps
            return TimeZoneSensitivity.AMBIGUOUS;
        } else if (input instanceof java.sql.Time) {
            return TimeZoneSensitivity.AMBIGUOUS;
        } else if (input instanceof java.util.Date) {
            // java.util.Date is essentially time zone sensitive (stores UTC timestamp)
            return TimeZoneSensitivity.SENSITIVE;
        } else if (input instanceof DingoDateTime) {
            return ((DingoDateTime) input).isTimeZoneSensitive()
                ? TimeZoneSensitivity.SENSITIVE : TimeZoneSensitivity.INSENSITIVE;
        }

        return TimeZoneSensitivity.UNKNOWN;
    }

    private static TimeZoneSensitivity analyzeForTimeZoneInsensitiveTarget(Object input, ZoneId contextZone) {
        // For time zone insensitive targets, most inputs are treated as insensitive
        if (input instanceof String) {
            return analyzeStringInput((String) input);
        } else if (input instanceof java.sql.Date) {
            return TimeZoneSensitivity.INSENSITIVE;
        } else if (input instanceof java.sql.Time) {
            return TimeZoneSensitivity.INSENSITIVE;
        } else if (input instanceof DingoDateTime) {
            return ((DingoDateTime) input).isTimeZoneSensitive()
                ? TimeZoneSensitivity.AMBIGUOUS : TimeZoneSensitivity.INSENSITIVE;
        }

        return TimeZoneSensitivity.AMBIGUOUS;
    }

    private static TimeZoneSensitivity analyzeStringInput(String input) {
        if (input == null || input.trim().isEmpty()) {
            return TimeZoneSensitivity.UNKNOWN;
        }

        // Analyze whether a string contains time zone information
        String trimmed = input.trim();
        if (trimmed.contains("+") || trimmed.contains("-") || trimmed.contains("Z") || trimmed.contains("T")) {
            if (trimmed.matches(".*[+-]\\d{2}:?\\d{2}") || trimmed.matches(".*[Zz]$")
                || (trimmed.contains("[") && trimmed.contains("]"))) {
                return TimeZoneSensitivity.SENSITIVE;
            }
        }
        return TimeZoneSensitivity.INSENSITIVE;
    }
}
