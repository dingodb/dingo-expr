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

package io.dingodb.expr.runtime.op.logical;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

@Operators
abstract class NotOp extends UnaryOp {
    private static final long serialVersionUID = 7750308004107964180L;

    static boolean not(boolean value) {
        return !value;
    }

    @Override
    public @NonNull OpType getOpType() {
        return OpType.NOT;
    }

    @Override
    public Object keyOf(@NonNull Type type) {
        if (Types.BOOL.matches(type)) {
            return Types.BOOL;
        }
        return null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        types[0] = Types.BOOL;
        return Types.BOOL;
    }
}
