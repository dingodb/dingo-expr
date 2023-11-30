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
import io.dingodb.expr.runtime.op.UnaryNumericOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Operators
abstract class Round1Fun extends UnaryNumericOp {
    public static final String NAME = "ROUND";

    private static final long serialVersionUID = -3008249205987880893L;

    static int round(int value) {
        return value;
    }

    static long round(long value) {
        return value;
    }

    static float round(float value) {
        return Math.round(value);
    }

    static double round(double value) {
        return Math.round(value);
    }

    static @NonNull BigDecimal round(@NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.HALF_UP);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
