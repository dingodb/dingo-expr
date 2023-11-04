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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Operators
abstract class CeilFun extends UnaryMathFun {
    public static final String NAME = "CEIL";

    private static final long serialVersionUID = -3077375964425561436L;

    static int ceil(int value) {
        return value;
    }

    static long ceil(long value) {
        return value;
    }

    static double ceil(double value) {
        return Math.ceil(value);
    }

    static @NonNull BigDecimal ceil(@NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.CEILING);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
