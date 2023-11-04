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

import io.dingodb.expr.runtime.op.VariadicOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

abstract class LogicalFun extends VariadicOp {
    private static final long serialVersionUID = -7427875713281737789L;

    @Override
    public Object keyOf(@NonNull Type @NonNull ... types) {
        return Arrays.stream(types).allMatch(t -> t.equals(Types.BOOL)) ? Types.BOOL : null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        Arrays.fill(types, Types.BOOL);
        return Types.BOOL;
    }

    @Override
    public final Type getType() {
        return Types.BOOL;
    }
}
