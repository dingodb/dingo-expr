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
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
abstract class StringCastOp extends CastOp {
    private static final long serialVersionUID = 5530720608036585479L;

    static @NonNull String stringCast(int value) {
        return Integer.toString(value);
    }

    static @NonNull String stringCast(long value) {
        return Long.toString(value);
    }

    static @NonNull String stringCast(float value) {
        return Float.toString(value);
    }

    static @NonNull String stringCast(double value) {
        return Double.toString(value);
    }

    static @NonNull String stringCast(boolean value) {
        return Boolean.toString(value);
    }

    static @NonNull String stringCast(@NonNull BigDecimal value) {
        return value.toString();
    }

    static @NonNull String stringCast(@NonNull String value) {
        return value;
    }

    static @NonNull String stringCast(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    static @NonNull String stringCast(Date value, @NonNull ExprConfig config) {
        return DateTimeUtils.dateFormat(value, config.getOutputDateFormatter());
    }

    static @NonNull String stringCast(Time value, @NonNull ExprConfig config) {
        return DateTimeUtils.timeFormat(value, config.getOutputTimeFormatter());
    }

    static @NonNull String stringCast(Timestamp value, @NonNull ExprConfig config) {
        return DateTimeUtils.timestampFormat(value, config.getOutputTimestampFormatter());
    }

    static @Nullable String stringCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.STRING;
    }
}
