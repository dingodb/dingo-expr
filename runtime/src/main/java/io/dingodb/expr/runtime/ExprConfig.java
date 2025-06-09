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
    };

    ExprConfig ADVANCED = new ExprConfig() {
        ExprContext exprContext = ExprContext.INVALID;

        @Override
        public boolean withSimplification() {
            return true;
        }

        @Override
        public boolean withRangeCheck() {
            return true;
        }

        public ExprContext getExprContext() {
            return exprContext;
        }

        public void setExprContext(ExprContext exprContext) {
            this.exprContext = exprContext;
        }
    };

    default void setExprContext(ExprContext exprContext) {
        return;
    }

    default ExprContext getExprContext() {
        return ExprContext.INVALID;
    }

    default boolean withSimplification() {
        return false;
    }

    default boolean withRangeCheck() {
        return false;
    }

    default boolean withGeneralOp() {
        return true;
    }

    default TimeZone getTimeZone() {
        return TimeZone.getDefault();
    }

    default DateTimeFormatter[] getParseDateFormatters() {
        return DateTimeUtils.DEFAULT_PARSE_DATE_FORMATTERS;
    }

    default DateTimeFormatter[] getParseTimeFormatters() {
        return DateTimeUtils.DEFAULT_PARSE_TIME_FORMATTERS;
    }

    default DateTimeFormatter[] getParseTimestampFormatters() {
        return DateTimeUtils.DEFAULT_PARSE_TIMESTAMP_FORMATTERS;
    }

    default DateTimeFormatter[] getParseDateAndTimestampFormatters() {
        return DateTimeUtils.DEFAULT_PARSE_DATE_AND_TIMESTAMP_FORMATTERS;
    }

    default DateTimeFormatter getOutputDateFormatter() {
        return DateTimeUtils.DEFAULT_OUTPUT_DATE_FORMATTER;
    }

    default DateTimeFormatter getOutputTimeFormatter() {
        return DateTimeUtils.DEFAULT_OUTPUT_TIME_FORMATTER;
    }

    default DateTimeFormatter getOutputTimestampFormatter() {
        return DateTimeUtils.DEFAULT_OUTPUT_TIMESTAMP_FORMATTER;
    }
}
