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
import io.dingodb.expr.runtime.op.BinaryNumericOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;

@Operators
abstract class ModFun extends BinaryNumericOp {
    public static final String NAME = "MOD";

    private static final long serialVersionUID = -1273869618861708383L;

    static @Nullable Integer mod(int value0, int value1) {
        return value1 != 0 ? value0 % value1 : null;
    }

    static @Nullable Long mod(long value0, long value1) {
        return value1 != 0 ? value0 % value1 : null;
    }

    static @Nullable BigDecimal mod(@NonNull BigDecimal value0, @NonNull BigDecimal value1) {
        return value1.compareTo(BigDecimal.ZERO) != 0 ? value0.remainder(value1) : null;
    }

    @Override
    public final @NonNull String getName() {
        return NAME;
    }
}
