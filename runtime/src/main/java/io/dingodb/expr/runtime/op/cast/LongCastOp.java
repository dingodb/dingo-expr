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
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Operators
abstract class LongCastOp extends CastOp {
    private static final long serialVersionUID = -5046347541755567683L;

    static long longCast(int value) {
        return value;
    }

    static long longCast(long value) {
        return value;
    }

    static long longCast(float value) {
        return Math.round((double) value);
    }

    static long longCast(double value) {
        return Math.round(value);
    }

    static long longCast(boolean value) {
        return value ? 1L : 0L;
    }

    static long longCast(@NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.HALF_UP).longValue();
    }

    static long longCast(@NonNull String value) {
        return Long.parseLong(value);
    }

    static long longCast(Void ignoredValue) {
        return 0L;
    }

    @Override
    public final Type getType() {
        return Types.LONG;
    }
}
