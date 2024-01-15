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

package io.dingodb.expr.rel.op;

import io.dingodb.expr.rel.AbstractRelOp;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOpVisitor;
import io.dingodb.expr.rel.TupleCompileContext;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

@EqualsAndHashCode(callSuper = true, of = {"projects"})
public final class ProjectOp extends AbstractRelOp implements PipeOp {
    public static final String NAME = "PROJECT";

    private static final long serialVersionUID = 9070704469597102368L;

    @Getter
    private Expr[] projects;

    public ProjectOp(Expr[] projects) {
        this.projects = projects;
    }

    @Override
    public Object @NonNull [] put(Object @NonNull [] tuple) {
        evalContext.setTuple(tuple);
        return Arrays.stream(projects)
            .map(p -> p.eval(evalContext, exprConfig))
            .toArray(Object[]::new);
    }

    @Override
    public void compile(TupleCompileContext context, @NonNull RelConfig config) {
        super.compile(context, config);
        ExprCompiler compiler = config.getExprCompiler();
        projects = Arrays.stream(projects)
            .map(p -> compiler.visit(p, context))
            .toArray(Expr[]::new);
        this.type = Types.tuple(Arrays.stream(projects)
            .map(Expr::getType)
            .toArray(Type[]::new));
    }

    @Override
    public <R, T> R accept(@NonNull RelOpVisitor<R, T> visitor, T obj) {
        return visitor.visitProjectOp(this, obj);
    }

    @Override
    public @NonNull String toString() {
        return NAME + ": " + Arrays.toString(projects);
    }
}
