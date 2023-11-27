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

import java.math.BigDecimal;
import java.sql.Timestamp;

@Operators
abstract class TimestampCastOp extends CastOp {
    private static final long serialVersionUID = -2472358326023215685L;

    static @NonNull Timestamp timestampCast(int value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Timestamp timestampCast(long value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Timestamp timestampCast(@NonNull BigDecimal value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    static @Nullable Timestamp timestampCast(String value, ExprConfig config) {
        if (config != null && config.getInTimestampFormatters() != null) {
            return DateTimeUtils.parseTimestamp(value, config.getInTimestampFormatters());
        }
        return DateTimeUtils.parseTimestamp(value);
    }

    static @NonNull Timestamp timestampCast(@NonNull Timestamp value) {
        return value;
    }

    static @Nullable Timestamp timestampCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.TIMESTAMP;
    }
}
