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

@Operators
abstract class AbsFun extends UnaryNumericOp {
    public static final String NAME = "ABS";

    private static final long serialVersionUID = -6818601820986123978L;

    static int abs(int num) {
        return Math.abs(num);
    }

    static long abs(long num) {
        return Math.abs(num);
    }

    static float abs(float num) {
        return Math.abs(num);
    }

    static double abs(double num) {
        return Math.abs(num);
    }

    static @NonNull BigDecimal abs(@NonNull BigDecimal num) {
        return num.abs();
    }

    @Override
    public final @NonNull String getName() {
        return NAME;
    }
}
