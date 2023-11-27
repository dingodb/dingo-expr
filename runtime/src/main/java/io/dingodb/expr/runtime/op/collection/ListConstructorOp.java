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
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.ExceptionUtils;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

public final class ListConstructorOp extends ListConstructorOpFactory {
    private static final long serialVersionUID = -7883906710460287752L;

    @Getter
    private final ListType type;

    ListConstructorOp(Type elementType) {
        super();
        type = Types.list(elementType);
    }

    @Override
    public @NonNull Object eval(@NonNull Expr @NonNull [] exprs, EvalContext context, ExprConfig config) {
        int size = exprs.length;
        List<Object> list = new ArrayList<>(size);
        for (Expr expr : exprs) {
            Object value = ExceptionUtils.nonNullElement(expr.eval(context, config));
            list.add(value);
        }
        return list;
    }

    @Override
    public Object getKey() {
        return type.getElementType();
    }
}
