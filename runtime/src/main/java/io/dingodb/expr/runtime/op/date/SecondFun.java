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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
abstract class SecondFun extends UnaryOp {
    public static final String NAME = "SECOND";
    private static final long serialVersionUID = 216520384536895803L;

    static int extractSecond(@NonNull Date value, ExprConfig config) {
        return DateTimeUtils.extractSecond(value);
    }

    static int extractSecond(@NonNull Time value, ExprConfig config) {
        return DateTimeUtils.extractSecond(value);
    }

    static int extractSecond(@NonNull Timestamp value, ExprConfig config) {
        return TimestampUtils.extractSecond(value);
    }

    static Integer extractSecond(String value, @NonNull ExprConfig config) {
        Time time = DateTimeUtils.parseTime(value, config.getParseTimeAndTimestampFormatters());
        if (time == null) {
            return null;
        }
        return DateTimeUtils.extractSecond(time);
    }

    static @Nullable Object extractSecond(Void value, @NonNull ExprConfig config) {
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
