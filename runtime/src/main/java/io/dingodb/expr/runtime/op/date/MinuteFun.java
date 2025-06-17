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
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.expr.runtime.utils.TimestampUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
abstract class MinuteFun extends UnaryOp {
    public static final String NAME = "MINUTE";
    private static final long serialVersionUID = 5813236374446762801L;

    static int extractMinute(@NonNull Date value, ExprConfig config) {
        return DateTimeUtils.extractMinute(value);
    }

    static int extractMinute(@NonNull Time value, ExprConfig config) {
        return DateTimeUtils.extractMinute(value);
    }

    static int extractMinute(@NonNull Timestamp value, ExprConfig config) {
        return TimestampUtils.extractMinute(value);
    }

    static int extractMinute(String value, @NonNull ExprConfig config) {
        Time time = DateTimeUtils.parseTime(value, config.getParseTimeAndTimestampFormatters());
        if (time == null) {
            return 0;
        }
        return DateTimeUtils.extractMinute(time);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
