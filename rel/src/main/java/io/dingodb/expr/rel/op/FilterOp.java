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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
@EqualsAndHashCode(callSuper = true, of = {"filter"})
public final class FilterOp extends AbstractRelOp implements PipeOp {
    public static final String NAME = "FILTER";

    private static final long serialVersionUID = 2278831820076088621L;

    @Getter
    private Expr filter;

    public FilterOp(Expr filter) {
        this.filter = filter;
    }

    @Override
    public Object @Nullable [] put(Object @NonNull [] tuple) {
        evalContext.setTuple(tuple);
        Object v = filter.eval(evalContext, exprConfig);
        return (v != null && (Boolean) v) ? tuple : null;
    }

    @Override
    public void compile(TupleCompileContext context, @NonNull RelConfig config) {
        super.compile(context, config);
        ExprCompiler compiler = config.getExprCompiler();
        if (log.isTraceEnabled()) {
            log.trace("filter = \"{}\".", filter.toDebugString());
        }
        filter = compiler.visit(filter, context);
        type = context.getType();
    }

    @Override
    public <R, T> R accept(@NonNull RelOpVisitor<R, T> visitor, T obj) {
        return visitor.visitFilterOp(this, obj);
    }

    @Override
    public @NonNull String toString() {
        return NAME + ": " + filter.toString();
    }
}
