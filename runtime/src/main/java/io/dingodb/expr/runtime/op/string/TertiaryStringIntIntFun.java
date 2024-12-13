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

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.op.TertiaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

abstract class TertiaryStringIntIntFun extends TertiaryOp {
    private static final long serialVersionUID = 8969966142892709405L;

    @Override
    public final @Nullable OpKey keyOf(@NonNull Type type0, @NonNull Type type1, @NonNull Type type2) {
        return OpKeys.STRING_INT_INT.keyOf(type0, type1, type2);
    }

    @Override
    public final @Nullable OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        return OpKeys.STRING_INT_INT.bestKeyOf(types);
    }
}
