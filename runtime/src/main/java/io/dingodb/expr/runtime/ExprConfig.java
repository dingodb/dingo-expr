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

package io.dingodb.expr.runtime;

import io.dingodb.expr.runtime.utils.DateTimeUtils;

import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

public interface ExprConfig {
    ExprConfig SIMPLE = new ExprConfig() {
        @Override
        public boolean withSimplification() {
            return false;
        }
    };

    ExprConfig ADVANCED = new ExprConfig() {
        @Override
        public boolean withRangeCheck() {
            return true;
        }
    };

    default boolean withSimplification() {
        return true;
    }

    default boolean withRangeCheck() {
        return false;
    }

    default TimeZone getTimeZone() {
        return TimeZone.getDefault();
    }

    default DateTimeFormatter[] getInDateFormatters() {
        return DateTimeUtils.DEFAULT_IN_DATE_FORMATTERS;
    }

    default DateTimeFormatter[] getInTimeFormatters() {
        return DateTimeUtils.DEFAULT_IN_TIME_FORMATTERS;
    }

    default DateTimeFormatter[] getInTimestampFormatters() {
        return DateTimeUtils.DEFAULT_IN_TIMESTAMP_FORMATTERS;
    }

    default DateTimeFormatter getOutDateFormatter() {
        return DateTimeUtils.DEFAULT_OUT_DATE_FORMATTER;
    }

    default DateTimeFormatter getOutTimeFormatter() {
        return DateTimeUtils.DEFAULT_OUT_TIME_FORMATTER;
    }

    default DateTimeFormatter getOutTimestampFormatter() {
        return DateTimeUtils.DEFAULT_OUT_TIMESTAMP_FORMATTER;
    }
}
