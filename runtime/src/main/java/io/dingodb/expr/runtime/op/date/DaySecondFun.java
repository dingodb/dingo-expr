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

package io.dingodb.expr.runtime.op.date;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.common.timezone.DateTimeUtils;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.sql.Timestamp;

@Operators
abstract class DaySecondFun extends UnaryOp {
    public static final String NAME = "DAY_SECOND";
    private static final long serialVersionUID = 2232950739310291472L;

    static int extractDaySecond(@NonNull Date value, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime dateTime = processor.getTierProcessor().convertInput(value, DateTimeType.TIMESTAMP);

        int day = processor.extractDay(dateTime);
        int hour = processor.extractHour(dateTime);
        int minute = processor.extractMinute(dateTime);
        int second = processor.extractSecond(dateTime);
        return DateTimeUtils.concatIntegers(new int[] {day, hour, minute, second});
    }

    static int extractDaySecond(@NonNull Timestamp value, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();

        int day = processor.extractDay(value);
        int hour = processor.extractHour(value);
        int minute = processor.extractMinute(value);
        int second = processor.extractSecond(value);
        return DateTimeUtils.concatIntegers(new int[] {day, hour, minute, second});
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
