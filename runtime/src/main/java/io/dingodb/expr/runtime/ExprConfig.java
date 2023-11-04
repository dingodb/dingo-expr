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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class ExprConfig {
    public static final ExprConfig TRIVIAL = new ExprConfig(
        TimeZone.getDefault(),
        DateTimeUtils.DEFAULT_IN_DATE_FORMATTERS,
        DateTimeUtils.DEFAULT_IN_TIME_FORMATTERS,
        DateTimeUtils.DEFAULT_IN_TIMESTAMP_FORMATTERS,
        DateTimeUtils.DEFAULT_OUT_DATE_FORMATTER,
        DateTimeUtils.DEFAULT_OUT_TIME_FORMATTER,
        DateTimeUtils.DEFAULT_OUT_TIMESTAMP_FORMATTER,
        false,
        false,
        TimeFormatStringStyle.JAVA
    );

    public static final ExprConfig FOR_SQL = new ExprConfig(
        TimeZone.getDefault(),
        DateTimeUtils.DEFAULT_IN_DATE_FORMATTERS,
        DateTimeUtils.DEFAULT_IN_TIME_FORMATTERS,
        DateTimeUtils.DEFAULT_IN_TIMESTAMP_FORMATTERS,
        DateTimeUtils.DEFAULT_OUT_DATE_FORMATTER,
        DateTimeUtils.DEFAULT_OUT_TIME_FORMATTER,
        DateTimeUtils.DEFAULT_OUT_TIMESTAMP_FORMATTER,
        true,
        true,
        TimeFormatStringStyle.SQL
    );

    @Getter
    private final TimeZone timeZone;
    @Getter
    private final DateTimeFormatter[] inDateFormatters;
    @Getter
    private final DateTimeFormatter[] inTimeFormatters;
    @Getter
    private final DateTimeFormatter[] inTimestampFormatters;
    @Getter
    private final DateTimeFormatter outDateFormatter;
    @Getter
    private final DateTimeFormatter outTimeFormatter;
    @Getter
    private final DateTimeFormatter outTimestampFormatter;
    @Getter
    private final boolean doSimplification;
    @Getter
    private final boolean doCastingCheck;
    @Getter
    private final TimeFormatStringStyle timeFormatStringStyle;

    public enum TimeFormatStringStyle {
        JAVA,
        SQL,
    }
}
