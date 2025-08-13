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
abstract class IntCastCheckOp extends CastOp {
    private static final long serialVersionUID = 2998309220568085549L;

    static int intCast(int value) {
        return value;
    }

    static int intCast(long value) {
        int r = (int) value;
        if (r == value) {
            return r;
        }
        throw new CastingException(Types.INT, Types.LONG, ExceptionUtils.exceedsIntRange());
    }

    static int intCast(float value) {
        int r = Math.round(value);
        if (Math.abs((float) r - value) <= 0.5f) {
            return r;
        }
        throw new CastingException(Types.INT, Types.FLOAT, ExceptionUtils.exceedsIntRange());
    }

    static int intCast(double value) {
        int r = (int) Math.round(value);
        if (Math.abs((double) r - value) <= 0.5) {
            return r;
        }
        throw new CastingException(Types.INT, Types.DOUBLE, ExceptionUtils.exceedsIntRange());
    }

    static int intCast(boolean value) {
        return value ? 1 : 0;
    }

    static int intCast(@NonNull BigDecimal value) {
        int r = value.setScale(0, RoundingMode.HALF_UP).intValue();
        BigDecimal err = BigDecimal.valueOf(r).subtract(value).abs();
        if (err.add(err).compareTo(BigDecimal.ONE) <= 0) {
            return r;
        }
        throw new CastingException(Types.INT, Types.DECIMAL, ExceptionUtils.exceedsIntRange());
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

    @Override
    public boolean doRangeChecking() {
        return true;
    }

    static int intCastWithStringCompat(@NonNull String value) {
        int result = 0;
        String val = value.trim();
        try {
            result = Integer.parseInt(val);
        } catch (NumberFormatException e) {
            int lastNumberPos = 0;
            if (value != null) {
                String v = trimDigitString(value);
                try {
                    result = Integer.parseInt(value.trim());
                } catch (NumberFormatException e1) {
                    result = 0;
                }
            }
        }

        return result;
    }
}
