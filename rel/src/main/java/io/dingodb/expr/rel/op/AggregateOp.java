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
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.CacheSupplier;
import io.dingodb.expr.rel.TupleCompileContext;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.TupleType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true, of = {"aggList"})
abstract class AggregateOp extends AbstractRelOp implements CacheOp {
    private static final long serialVersionUID = 7414468955475481236L;

    @Getter
    protected final List<Expr> aggList;

    protected final transient CacheSupplier cacheSupplier;

    protected AggregateOp(
        TupleType type,
        TupleEvalContext evalContext,
        ExprConfig exprConfig,
        List<Expr> aggList,
        CacheSupplier cacheSupplier
    ) {
        super(type, evalContext, exprConfig);
        this.aggList = aggList;
        this.cacheSupplier = cacheSupplier;
    }

    protected List<Expr> compileAggList(TupleCompileContext context, @NonNull ExprCompiler compiler) {
        return aggList.stream().map(agg -> compiler.visit(agg, context)).collect(Collectors.toList());
    }
}
