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
import io.dingodb.expr.rel.utils.ArrayUtils;
import io.dingodb.expr.runtime.expr.AggExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@EqualsAndHashCode(callSuper = true, of = {"groupIndices"})
public final class GroupedAggregateOp extends AggregateOp {
    public static final String NAME = "AGG";

    private static final long serialVersionUID = 861648134033011738L;

    @Getter
    private final int[] groupIndices;

    private final Map<TupleKey, Object[]> cache;

    public GroupedAggregateOp(int @NonNull [] groupIndices, List<Expr> aggList) {
        super(aggList);
        this.groupIndices = groupIndices;
        this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public void put(Object @NonNull [] tuple) {
        Object[] keyTuple = ArrayUtils.map(tuple, groupIndices);
        Object[] vars = cache.computeIfAbsent(new TupleKey(keyTuple), k -> new Object[aggList.size()]);
        evalContext.setTuple(tuple);
        for (int i = 0; i < vars.length; ++i) {
            AggExpr aggExpr = (AggExpr) aggList.get(i);
            vars[i] = aggExpr.add(vars[i], evalContext, exprConfig);
        }
    }

    @Override
    public @NonNull Stream<Object[]> get() {
        return cache.entrySet().stream()
            .map(e -> ArrayUtils.concat(e.getKey().getTuple(), e.getValue()));
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void compile(TupleCompileContext context, @NonNull RelConfig config) {
        super.compile(context, config);
        this.type = Types.tuple(
            ArrayUtils.concat(
                ArrayUtils.map(context.getType().getTypes(), groupIndices),
                aggList.stream().map(Expr::getType).toArray(Type[]::new)
            )
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

    /**
     * Wrap tuples to provide hash and equals.
     */
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    public static class TupleKey {
        @Getter
        private final Object[] tuple;

        @Override
        public int hashCode() {
            return Arrays.hashCode(tuple);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TupleKey) {
                return Arrays.equals(this.tuple, ((TupleKey) obj).tuple);
            }
            return false;
        }
    }
}
