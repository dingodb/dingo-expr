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

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.exception.ElementNotExist;
import io.dingodb.expr.runtime.exception.NeverRunHere;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.ExprVisitor;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class VarStub implements Expr {
    private static final long serialVersionUID = -4099878931569129325L;

    @Getter
    private final CompileContext context;

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        throw new NeverRunHere();
    }

    @Override
    public Type getType() {
        return null;
    }

    @Override
    public @NonNull Expr simplify(ExprConfig config) {
        throw new NeverRunHere();
    }

    @Override
    public <R, T> R accept(@NonNull ExprVisitor<R, T> visitor, T obj) {
        throw new NeverRunHere();
    }

    @Override
    public @NonNull String toDebugString() {
        return getClass().getSimpleName() + "[" + context + "]";
    }

    public Expr getElement(Object index) {
        CompileContext child = context.getChild(index);
        if (child != null) {
            return VarFactory.createVar(child);
        }
        throw new ElementNotExist(index, context);
    }
}
