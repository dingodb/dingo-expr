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
import io.dingodb.expr.runtime.exception.ExprEvaluatingException;
import io.dingodb.expr.runtime.exception.NullElementsNotAllowed;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.MapType;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;

public final class MapConstructorOp extends MapConstructorOpFactory {
    private static final long serialVersionUID = 7767648022061360937L;

    @Getter
    private final MapType type;

    MapConstructorOp(MapType type) {
        super();
        this.type = type;
    }

    @Override
    public @NonNull Object eval(@NonNull Expr @NonNull [] exprs, EvalContext context, ExprConfig config) {
        int size = exprs.length;
        if (size % 2 != 0) {
            throw new ExprEvaluatingException("Map constructor requires even parameters.");
        }
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < size; i += 2) {
            Object key = exprs[i].eval(context, config);
            if (key == null) {
                throw new NullElementsNotAllowed();
            }
            Object value = exprs[i + 1].eval(context, config);
            if (value == null) {
                throw new NullElementsNotAllowed();
            }
            map.put(key, value);
        }
        return map;
    }

    @Override
    public Object getKey() {
        return type;
    }
}
