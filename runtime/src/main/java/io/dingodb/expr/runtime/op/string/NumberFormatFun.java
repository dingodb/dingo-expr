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

package io.dingodb.expr.runtime.op.string;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Operators
abstract class NumberFormatFun extends BinaryOp {
    public static final String NAME = "FORMAT";

    private static final long serialVersionUID = 4805636716328583550L;

    static String format(@NonNull BigDecimal value, int scale) {
        if (scale < 0) {
            scale = 0;
        }
        return value.setScale(scale, RoundingMode.HALF_UP).toString();
    }

    @Override
    public final @Nullable OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return OpKeys.DECIMAL_INT.keyOf(type0, type1);
    }

    @Override
    public final @Nullable OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        return OpKeys.DECIMAL_INT.bestKeyOf(types);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
