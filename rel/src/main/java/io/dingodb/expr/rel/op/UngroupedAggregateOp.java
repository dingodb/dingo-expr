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

import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOpVisitor;
import io.dingodb.expr.rel.TupleCompileContext;
import io.dingodb.expr.runtime.expr.AggExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Stream;

public final class UngroupedAggregateOp extends AggregateOp {
    public static final String NAME = "AGG";

    private static final long serialVersionUID = -4719541350568819417L;

    private transient Object[] vars;

    public UngroupedAggregateOp(List<Expr> aggList) {
        super(aggList);
    }

    @Override
    public void put(Object @NonNull [] tuple) {
        if (vars == null) {
            vars = new Object[aggList.size()];
        }
        evalContext.setTuple(tuple);
        for (int i = 0; i < vars.length; ++i) {
            AggExpr aggExpr = (AggExpr) aggList.get(i);
            vars[i] = aggExpr.add(vars[i], evalContext, exprConfig);
        }
    }

    @Override
    public @NonNull Stream<Object[]> get() {
        if (vars != null) {
            return Stream.<Object[]>of(vars);
        }
        return Stream.<Object[]>of(
            aggList.stream()
                .map(agg -> ((AggExpr) agg).emptyValue())
                .toArray(Object[]::new)
        );
    }

    @Override
    public void clear() {
        vars = null;
    }

    @Override
    public void compile(TupleCompileContext context, @NonNull RelConfig config) {
        super.compile(context, config);
        this.type = Types.tuple(aggList.stream().map(Expr::getType).toArray(Type[]::new));
    }

    @Override
    public <R, T> R accept(@NonNull RelOpVisitor<R, T> visitor, T obj) {
        return visitor.visitUngroupedAggregateOp(this, obj);
    }

    @Override
    public @NonNull String toString() {
        return NAME + ": " + aggList.toString();
    }
}
