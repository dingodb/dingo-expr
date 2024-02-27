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

package io.dingodb.expr.runtime.op.collection;

import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.VariadicOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

abstract class CollectionConstructorOpFactory extends VariadicOp {
    private static final long serialVersionUID = -5648876774812972099L;

    @Override
    public OpKey keyOf(@NonNull Type @NonNull ... types) {
        long c = Arrays.stream(types).distinct().count();
        if (c == 1) {
            return types[0];
        } else if (c == 0) {
            return Types.ANY;
        }
        return null;
    }

    @Override
    public OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        Type best = Types.bestType(types);
        if (best != null) {
            Arrays.fill(types, best);
        }
        return best;
    }
}
