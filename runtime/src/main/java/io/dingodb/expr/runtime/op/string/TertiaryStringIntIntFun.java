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

import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

abstract class TertiaryStringIntIntFun extends TertiaryOp {
    private static final long serialVersionUID = 8969966142892709405L;

    @Override
    public Object keyOf(@NonNull Type type0, @NonNull Type type1, @NonNull Type type2) {
        if (type0.equals(Types.STRING) && type1.equals(Types.INT) && type2.equals(Types.INT)) {
            return Types.STRING;
        }
        return null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        if (types[0].equals(Types.STRING)) {
            types[1] = Types.INT;
            types[2] = Types.INT;
            return Types.STRING;
        }
        return null;
    }
}
