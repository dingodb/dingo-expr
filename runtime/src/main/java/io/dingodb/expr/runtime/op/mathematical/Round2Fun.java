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

package io.dingodb.expr.runtime.op.mathematical;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.exception.ExprEvaluatingException;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Operators
abstract class Round2Fun extends BinaryOp {
    public static final String NAME = "ROUND";

    private static final long serialVersionUID = 3584407297871012095L;

    static int round(int value, int scale) {
        if (scale >= 0) {
            return value;
        }
        return round(BigDecimal.valueOf(value), scale).intValue();
    }

    static long round(long value, int scale) {
        if (scale >= 0) {
            return value;
        }
        return round(BigDecimal.valueOf(value), scale).longValue();
    }

    static double round(double value, int scale) {
        return round(BigDecimal.valueOf(value), scale).doubleValue();
    }

    static @NonNull BigDecimal round(@NonNull BigDecimal value, int scale) {
        if (-10 <= scale && scale <= 10) {
            BigDecimal result = value.setScale(scale, RoundingMode.HALF_UP);
            if (scale < 0) {
                result = result.setScale(0, RoundingMode.HALF_UP);
            }
            return result;
        }
        throw new ExprEvaluatingException(
            new ArithmeticException("Scale of " + NAME + " function must be in range [-10, 10].")
        );
    }

    @Override
    public Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        return Types.INT.matches(type1) ? type0 : null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        Type best = Types.bestType(types[0]);
        if (best != null && best.isNumeric()) {
            types[0] = best;
            types[1] = Types.INT;
            return types[0];
        }
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
