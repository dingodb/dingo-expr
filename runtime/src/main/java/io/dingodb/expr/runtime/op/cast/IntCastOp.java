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
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;

//@Operators
abstract class IntCastOp extends CastOp {
    private static final long serialVersionUID = 6934382536298574928L;

    static int intCast(int value) {
        return value;
    }

    static int intCast(long value) {
        return (int) value;
    }

    static int intCast(float value) {
        return Math.round(value);
    }

    static int intCast(double value) {
        return (int) Math.round(value);
    }

    static int intCast(boolean value) {
        return value ? 1 : 0;
    }

    static int intCast(@NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.HALF_UP).intValue();
    }

    static int intCast(@NonNull String value) {
        return Integer.parseInt(value);
    }

    static @Nullable Integer intCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.INT;
    }
}
