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

import io.dingodb.expr.rel.CacheSupplier;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOpVisitor;
import io.dingodb.expr.rel.TupleCompileContext;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.expr.AggExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@EqualsAndHashCode(callSuper = true)
public final class UngroupedAggregateOp extends AggregateOp {
    public static final String NAME = "AGG";

    private static final long serialVersionUID = -4719541350568819417L;

    private transient Object[] vars;

    public UngroupedAggregateOp(List<Expr> aggList) {
        this(null, null, null, aggList, null);
    }

    private UngroupedAggregateOp(
        TupleType type,
        TupleEvalContext evalContext,
        ExprConfig exprConfig,
        List<Expr> aggList,
        CacheSupplier cacheSupplier
    ) {
        super(type, evalContext, exprConfig, aggList, cacheSupplier);
        vars = null;
    }


    private Object[] createVars() {
        vars = cacheSupplier.item(aggList.size());
        return vars;
    }

    @Override
    public synchronized void put(Object @NonNull [] tuple) {
        assert cacheSupplier != null
            : "Cache not initialized, call `this.setCache` first.";
        calc(vars, tuple, this::createVars);
        if (log.isTraceEnabled()) {
            log.trace("Input: {}", Arrays.toString(tuple));
        }
    }

    @Override
    public synchronized @NonNull Stream<Object[]> get() {
        if (vars != null) {
            if (log.isTraceEnabled()) {
                log.trace("Result in cache: {}", Arrays.toString(vars));
            }
            return Stream.<Object[]>of(
                IntStream.range(0, vars.length)
                    .mapToObj(i -> vars[i] != null ? vars[i] : ((AggExpr) aggList.get(i)).emptyValue())
                    .toArray()
            );
        }
        if (log.isTraceEnabled()) {
            log.trace("No result in cache.");
        }
        return Stream.<Object[]>of(
            aggList.stream()
                .map(agg -> ((AggExpr) agg).emptyValue())
                .toArray(Object[]::new)
        );
    }

    @Override
    public synchronized void reduce(Object @NonNull [] tuple) {
        merge(vars, tuple, 0, this::createVars);
    }

    @Override
    public synchronized void clear() {
        vars = null;
        if (log.isTraceEnabled()) {
            log.trace("Cache cleared.");
        }
    }

    @Override
    public @NonNull UngroupedAggregateOp compile(@NonNull TupleCompileContext context, @NonNull RelConfig config) {
        ExprCompiler compiler = config.getExprCompiler();
        List<Expr> compiledAggList = compileAggList(context, compiler);
        TupleType newType = Types.tuple(aggList.stream().map(Expr::getType).toArray(Type[]::new));
        return new UngroupedAggregateOp(
            newType,
            config.getEvalContext(),
            compiler.getConfig(),
            compiledAggList,
            config.getCacheSupplier()
        );
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
