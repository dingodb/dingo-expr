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

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.ArrayType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.ExceptionUtils;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Array;

public final class ArrayConstructorOp extends ArrayConstructorOpFactory {
    private static final long serialVersionUID = -826953730978206304L;

    @Getter
    private final ArrayType type;

    ArrayConstructorOp(Type elementType) {
        super();
        type = Types.array(elementType);
    }

    @Override
    public Object eval(@NonNull Expr @NonNull [] exprs, EvalContext context, ExprConfig config) {
        int size = exprs.length;
        Object array = ArrayBuilder.INSTANCE.visit(type.getElementType(), size);
        for (int i = 0; i < size; ++i) {
            Object value = ExceptionUtils.nonNullElement(exprs[i].eval(context, config));
            Array.set(array, i, value);
        }
        return array;
    }

    @Override
    public OpKey getKey() {
        return type.getElementType();
    }
}
