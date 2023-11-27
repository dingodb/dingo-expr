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

package io.dingodb.expr.runtime.op.cast;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Time;

@Operators
abstract class TimeCastOp extends CastOp {
    private static final long serialVersionUID = -7175316466368557014L;

    static @NonNull Time timeCast(int value) {
        return new Time(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Time timeCast(long value) {
        return new Time(DateTimeUtils.fromSecond(value));
    }

    static @Nullable Time timeCast(String value, ExprConfig config) {
        if (config != null && config.getInTimeFormatters() != null) {
            return DateTimeUtils.parseTime(value, config.getInTimeFormatters());
        }
        return DateTimeUtils.parseTime(value);
    }

    static @NonNull Time timeCast(@NonNull Time value) {
        return value;
    }

    static @Nullable Time timeCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.TIME;
    }
}
