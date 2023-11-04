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

import java.util.Arrays;

public abstract class BinaryNumericOp extends BinaryOp {
    private static final long serialVersionUID = -3432586934529603722L;

    @Override
    public final Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        if (type0.equals(Types.NULL)) {
            if (type1.equals(Types.NULL)) {
                return Types.INT;
            }
            return type1;
        }
        if (type1.equals(Types.NULL)) {
            return type0;
        }
        if (type0.equals(type1)) {
            return type0;
        }
        return null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        Type best;
        if (types[0].equals(Types.DECIMAL) || types[1].equals(Types.DECIMAL)) {
            best = Types.DECIMAL;
        } else if (types[0].equals(Types.DOUBLE) || types[1].equals(Types.DOUBLE)) {
            best = Types.DOUBLE;
        } else if (types[0].equals(Types.FLOAT) || types[1].equals(Types.FLOAT)) {
            best = Types.FLOAT;
        } else if (types[0].equals(Types.LONG) || types[1].equals(Types.LONG)) {
            best = Types.LONG;
        } else if (types[0].equals(Types.INT) || types[1].equals(Types.INT)) {
            best = Types.INT;
        } else {
            return null;
        }
        Arrays.fill(types, best);
        return best;
    }
}
