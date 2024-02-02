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
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

@Operators
abstract class PowFun extends BinaryOp {
    public static final String NAME = "POW";

    private static final long serialVersionUID = 6371448137108545795L;

    static double pow(double value0, double value1) {
        return Math.pow(value0, value1);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        if (Types.DOUBLE.matches(type0) && Types.DOUBLE.matches(type1)) {
            return Types.DOUBLE;
        }
        return null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        Arrays.fill(types, Types.DOUBLE);
        return Types.DOUBLE;
    }
}
