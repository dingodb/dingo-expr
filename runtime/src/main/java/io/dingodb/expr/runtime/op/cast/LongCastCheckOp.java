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
import io.dingodb.expr.runtime.exception.CastingException;
import io.dingodb.expr.runtime.utils.ExceptionUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;

//@Operators
abstract class LongCastCheckOp extends CastOp {
    private static final long serialVersionUID = -4999428444641603223L;

    static long longCast(int value) {
        return value;
    }

    static long longCast(long value) {
        return value;
    }

    static long longCast(float value) {
        long r = Math.round((double) value);
        if (Math.abs((float) r - value) <= 0.5f) {
            return r;
        }
        throw new CastingException(Types.LONG, Types.FLOAT, ExceptionUtils.exceedsLongRange());
    }

    static long longCast(double value) {
        long r = Math.round(value);
        if (Math.abs((double) r - value) <= 0.5) {
            return r;
        }
        throw new CastingException(Types.LONG, Types.DOUBLE, ExceptionUtils.exceedsLongRange());
    }

    static long longCast(boolean value) {
        return value ? 1L : 0L;
    }

    static long longCast(@NonNull BigDecimal value) {
        long r = value.setScale(0, RoundingMode.HALF_UP).longValue();
        BigDecimal err = BigDecimal.valueOf(r).subtract(value).abs();
        if (err.add(err).compareTo(BigDecimal.ONE) <= 0) {
            return r;
        }
        throw new CastingException(Types.LONG, Types.DECIMAL, ExceptionUtils.exceedsLongRange());
    }

    static long longCast(@NonNull String value) {
        return Long.parseLong(value);
    }

    static @Nullable Long longCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.LONG;
    }

    @Override
    public boolean doRangeChecking() {
        return true;
    }
}
