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

import java.io.Serial;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
public class QuarterFun extends UnaryOp {
    public static final String NAME = "QUARTER";
    @Serial
    private static final long serialVersionUID = -5306012511147355441L;

    static int extractQuarter(@NonNull Date value, ExprConfig config) {
        return DateTimeUtils.extractQuarter(value);
    }

    static int extractQuarter(@NonNull Timestamp value, ExprConfig config) {
        return TimestampUtils.extractQuarter(value);
    }

    static Integer extractQuarter(String value, @NonNull ExprConfig config) {
        Date date = DateTimeUtils.parseDate(value, config.getParseDateAndTimestampFormatters());
        if (date == null) {
            return null;
        }
        return DateTimeUtils.extractQuarter(date);
    }

    static @Nullable Integer extractQuarter(Time value, @NonNull ExprConfig config) {
        return null;
    }

    static @Nullable Object extractQuarter(Void value, @NonNull ExprConfig config) {
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
