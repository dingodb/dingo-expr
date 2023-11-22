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
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

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
    public Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        if (Types.DECIMAL.isCompatible(type0) && Types.INT.isCompatible(type1)) {
            return type0;
        }
        return null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        if (types[0].isNumeric() && types[1].isNumeric()) {
            types[0] = Types.DECIMAL;
            types[1] = Types.INT;
        }
        return Types.DECIMAL;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
