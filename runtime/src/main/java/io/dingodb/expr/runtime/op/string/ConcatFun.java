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

@Operators(nullable = true)
abstract class ConcatFun extends BinaryOp {
    public static final String NAME = "CONCAT";

    private static final long serialVersionUID = 5454356467741754567L;

    static String concat(String value0, String value1) {
        if (value0 != null) {
            if (value1 != null) {
                return value0 + value1;
            }
            return value0;
        }
        return value1;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        return ((type0.equals(Types.STRING) || type0.equals(Types.NULL))
            && (type1.equals(Types.STRING) || type1.equals(Types.NULL))) ? Types.STRING : null;
    }
}
