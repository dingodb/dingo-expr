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

import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.SourceOp;
import io.dingodb.expr.runtime.expr.Expr;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RelOpBuilder {
    public static @NonNull InitBuilder builder() {
        return new InitBuilder();
    }

    public static @NonNull Builder<?, ?> builder(RelOp op) {
        if (op == null) {
            return builder();
        } else if (op instanceof PipeOp) {
            return new PipeBuilder((PipeOp) op);
        } else if (op instanceof CacheOp) {
            return new CacheBuilder((CacheOp) op);
        } else if (op instanceof SourceOp) {
            return new SourceBuilder((SourceOp) op);
        }
        throw new IllegalArgumentException("Unsupported rel op class \"" + op.getClass().getCanonicalName() + "\".");
    }

    @AllArgsConstructor(access = AccessLevel.PROTECTED)
    public abstract static class Builder<P extends RelOp, B extends Builder<P, B>> {
        protected transient P relOp;

        protected abstract Builder<?, ?> tandemBuilder(P op0, PipeOp op1);

        protected abstract Builder<?, ?> tandemBuilder(P op0, CacheOp op1);

        public Builder<?, ?> project(Expr @NonNull ... projects) {
            return tandemBuilder(relOp, new ProjectOp(projects));
        }

        public Builder<?, ?> filter(Expr filter) {
            return tandemBuilder(relOp, new FilterOp(filter));
        }

        public Builder<?, ?> agg(Expr... aggExprs) {
            return agg(null, aggExprs);
        }

        public Builder<?, ?> agg(int[] groupIndices, Expr... aggExprs) {
            return agg(groupIndices, Arrays.asList(aggExprs));
        }

        public Builder<?, ?> agg(int[] groupIndices, List<Expr> aggList) {
            AggregateOp op;
            if (groupIndices != null) {
                op = new GroupedAggregateOp(groupIndices, aggList);
            } else {
                op = new UngroupedAggregateOp(aggList);
            }
            return tandemBuilder(relOp, op);
        }

        public P build() {
            return relOp;
        }
    }

    public static final class InitBuilder extends Builder<RelOp, InitBuilder> {
        private InitBuilder() {
            super(null);
        }

        @Override
        protected @NonNull PipeBuilder tandemBuilder(RelOp op0, PipeOp op1) {
            return new PipeBuilder(op1);
        }

        @Override
        protected @NonNull CacheBuilder tandemBuilder(RelOp op0, CacheOp op1) {
            return new CacheBuilder(op1);
        }

        @SuppressWarnings("MethodMayBeStatic")
        public @NonNull SourceBuilder values(List<Object @NonNull []> values) {
            return new SourceBuilder(new ValuesOp(values));
        }

        @SuppressWarnings("MethodMayBeStatic")
        public @NonNull SourceBuilder values(InputStream is) {
            return new SourceBuilder(new CsvValuesOp(is));
        }

        @SuppressWarnings("MethodMayBeStatic")
        public @NonNull SourceBuilder values(String... csvLines) {
            return new SourceBuilder(new CsvValuesOp(csvLines));
        }
    }

    public static final class SourceBuilder extends Builder<SourceOp, SourceBuilder> {
        private SourceBuilder(SourceOp relOp) {
            super(relOp);
        }

        @Override
        protected @NonNull SourceBuilder tandemBuilder(SourceOp op0, PipeOp op1) {
            relOp = new TandemSourcePipeOp(op0, op1);
            return this;
        }

        @Override
        protected @NonNull SourceBuilder tandemBuilder(SourceOp op0, CacheOp op1) {
            relOp = new TandemSourceCacheOp(relOp, op1);
            return this;
        }
    }

    public static final class PipeBuilder extends Builder<PipeOp, PipeBuilder> {
        private PipeBuilder(PipeOp relOp) {
            super(relOp);
        }

        @Override
        protected @NonNull PipeBuilder tandemBuilder(PipeOp op0, PipeOp op1) {
            relOp = new TandemPipePipeOp(op0, op1);
            return this;
        }

        @Override
        protected @NonNull CacheBuilder tandemBuilder(PipeOp op0, CacheOp op1) {
            CacheOp newOp = new TandemPipeCacheOp(relOp, op1);
            return new CacheBuilder(newOp);
        }
    }

    public static final class CacheBuilder extends Builder<CacheOp, CacheBuilder> {
        private CacheBuilder(CacheOp relOp) {
            super(relOp);
        }

        @Override
        protected @NonNull CacheBuilder tandemBuilder(CacheOp op0, PipeOp op1) {
            relOp = new TandemCachePipeOp(op0, op1);
            return this;
        }

        @Override
        protected @NonNull CacheBuilder tandemBuilder(CacheOp op0, CacheOp op1) {
            relOp = new TandemCacheCacheOp(op0, op1);
            return this;
        }
    }
}
