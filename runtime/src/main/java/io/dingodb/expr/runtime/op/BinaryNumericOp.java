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

package io.dingodb.expr.runtime.op;

import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

public abstract class BinaryNumericOp extends BinaryOp {
    private static final long serialVersionUID = -3432586934529603722L;

    @Override
    public final @Nullable Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        return (type0.equals(type1)) ? type0 : null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        Type best = Types.bestType(types[0], types[1]);
        if (best != null) {
            Arrays.fill(types, best);
        }
        return best;
    }
}
