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
abstract class FloorFun extends UnaryNumericOp {
    public static final String NAME = "FLOOR";

    private static final long serialVersionUID = -3433872871753770106L;

    static int floor(int value) {
        return value;
    }

    static long floor(long value) {
        return value;
    }

    static float floor(float value) {
        return (float) Math.floor(value);
    }

    static double floor(double value) {
        return Math.floor(value);
    }

    static @NonNull BigDecimal floor(@NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.FLOOR);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
