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
import io.dingodb.expr.rel.TupleKey;
import io.dingodb.expr.rel.utils.ArrayUtils;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@EqualsAndHashCode(callSuper = true, of = {"groupIndices"})
public final class GroupedAggregateOp extends AggregateOp {
    public static final String NAME = "AGG";

    private static final long serialVersionUID = 861648134033011738L;

    @Getter
    private final int[] groupIndices;

    private final transient Map<TupleKey, Object[]> cache;

    public GroupedAggregateOp(int @NonNull [] groupIndices, List<Expr> aggList) {
        this(null, null, null, groupIndices, aggList, null);
    }

    private GroupedAggregateOp(
        TupleType type,
        TupleEvalContext evalContext,
        ExprConfig exprConfig,
        int @NonNull [] groupIndices,
        List<Expr> aggList,
        CacheSupplier cacheSupplier
    ) {
        super(type, evalContext, exprConfig, aggList, cacheSupplier);
        this.groupIndices = groupIndices;
        cache = cacheSupplier != null ? cacheSupplier.cache() : null;
    }

    private Object[] createVars(TupleKey tupleKey) {
        Object[] vars = cacheSupplier.item(aggList.size());
        cache.put(tupleKey, vars);
        return vars;
    }

    @Override
    public synchronized void put(Object @NonNull [] tuple) {
        assert cacheSupplier != null && cache != null
            : "Cache not initialized, call `this.setCache` first.";
        TupleKey tupleKey = new TupleKey(ArrayUtils.map(tuple, groupIndices));
        calc(cache.get(tupleKey), tuple, () -> createVars(tupleKey));
        if (log.isTraceEnabled()) {
            log.trace("Input: {}", Arrays.toString(tuple));
        }
    }

    @Override
    public synchronized @NonNull Stream<Object[]> get() {
        if (log.isTraceEnabled()) {
            if (cache.isEmpty()) {
                log.trace("No result in cache.");
            } else {
                String content =
                    "{"
                    + cache.entrySet().stream()
                        .map(e -> e.getKey() + ": " + Arrays.toString(e.getValue()))
                        .collect(Collectors.joining(", "))
                    + "}";
                log.trace("Result in cache: {} items, {}", cache.size(), content);
            }
        }
        return cache.entrySet().stream()
            .map(e -> ArrayUtils.concat(e.getKey().getTuple(), e.getValue()));
    }

    @Override
    public synchronized void reduce(Object @NonNull [] tuple) {
        int length = groupIndices.length;
        TupleKey tupleKey = new TupleKey(Arrays.copyOf(tuple, length));
        merge(cache.get(tupleKey), tuple, length, () -> createVars(tupleKey));
    }

    @Override
    public synchronized void clear() {
        cache.clear();
        if (log.isTraceEnabled()) {
            log.trace("Cache cleared.");
        }
    }

    @Override
    public @NonNull GroupedAggregateOp compile(@NonNull TupleCompileContext context, @NonNull RelConfig config) {
        ExprCompiler compiler = config.getExprCompiler();
        List<Expr> compiledAggList = compileAggList(context, compiler);
        TupleType newType = Types.tuple(
            ArrayUtils.concat(
                ArrayUtils.map(context.getType().getTypes(), groupIndices),
                compiledAggList.stream().map(Expr::getType).toArray(Type[]::new)
            )
        );
        return new GroupedAggregateOp(
            newType,
            config.getEvalContext(),
            compiler.getConfig(),
            groupIndices,
            compiledAggList,
            config.getCacheSupplier()
        );
    }

    @Override
    public <R, T> R accept(@NonNull RelOpVisitor<R, T> visitor, T obj) {
        return visitor.visitGroupedAggregateOp(this, obj);
    }

    @Override
    public @NonNull String toString() {
        return NAME + ": " + Arrays.toString(groupIndices) + aggList.toString();
    }
}
