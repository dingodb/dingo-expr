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

import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.ArrayType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SliceOpFactory extends BinaryOp {
    public static final SliceOpFactory INSTANCE = new SliceOpFactory();
    public static final String NAME = "SLICE";

    private static final long serialVersionUID = -7538757587959745023L;

    @Override
    public final @Nullable OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return Types.INT.matches(type1) ? type0 : null;
    }

    @Override
    public final OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        types[1] = Types.INT;
        return types[0];
    }

    @Override
    public BinaryOp getOp(OpKey key) {
        if (key != null) {
            if (key instanceof ArrayType) {
                return SliceArrayOp.of((ArrayType) key);
            } else if (key instanceof ListType) {
                return SliceListOp.of((ListType) key);
            }
        }
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
