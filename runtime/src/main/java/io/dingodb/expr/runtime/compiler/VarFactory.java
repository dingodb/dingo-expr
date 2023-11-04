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

package io.dingodb.expr.runtime.compiler;

import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.exception.ElementNotExist;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.Var;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class VarFactory {
    private VarFactory() {
    }

    public static @NonNull Expr of(@NonNull Object id, @NonNull CompileContext context) {
        if (id.equals(Var.WHOLE_VAR)) {
            return createVar(context);
        }
        CompileContext child = context.getChild(id);
        if (child != null) {
            return createVar(child);
        }
        throw new ElementNotExist(id, context);
    }

    static @NonNull Expr createVar(@NonNull CompileContext context) {
        Object id = context.getId();
        if (id != null) {
            return Exprs.var(id, context.getType());
        }
        return new VarStub(context);
    }
}
