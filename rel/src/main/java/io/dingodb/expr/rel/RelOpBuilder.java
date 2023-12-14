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

package io.dingodb.expr.rel;

import io.dingodb.expr.parser.ExprParser;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.rel.op.CsvValuesOp;
import io.dingodb.expr.rel.op.FilterOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.TandemPipeOp;
import io.dingodb.expr.rel.op.TandemSourceOp;
import io.dingodb.expr.rel.op.ValuesOp;
import io.dingodb.expr.runtime.expr.Expr;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.InputStream;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RelOpBuilder {
    private final ExprParser parser;

    public static @NonNull RelOpBuilder builder(@NonNull RelConfig config) {
        return new RelOpBuilder(config.getExprParser());
    }

    @SuppressWarnings("MethodMayBeStatic")
    private @NonNull FilterOp createFilter(Expr filter) {
        return new FilterOp(filter);
    }

    private @NonNull FilterOp createFilter(String filter) throws ExprParseException {
        return new FilterOp(parser.parse(filter));
    }

    @SuppressWarnings({"MethodMayBeStatic"})
    private @NonNull ProjectOp createProject(Expr[] projects) {
        return new ProjectOp(projects);
    }

    private @NonNull ProjectOp createProject(String @NonNull ... projects) throws ExprParseException {
        Expr[] exprs = new Expr[projects.length];
        for (int i = 0; i < projects.length; ++i) {
            exprs[i] = parser.parse(projects[i]);
        }
        return new ProjectOp(exprs);
    }

    public SourceBuilder values(List<Object @NonNull []> values) {
        return new SourceBuilder(new ValuesOp(values));
    }

    public SourceBuilder values(InputStream is) {
        return new SourceBuilder(new CsvValuesOp(is));
    }

    public SourceBuilder values(String... csvLines) {
        return new SourceBuilder(new CsvValuesOp(csvLines));
    }

    public PipeBuilder filter(Expr filter) {
        return new PipeBuilder(createFilter(filter));
    }

    public PipeBuilder filter(String filter) throws ExprParseException {
        return new PipeBuilder(createFilter(filter));
    }

    public PipeBuilder project(Expr @NonNull ... projects) {
        return new PipeBuilder(createProject(projects));
    }

    public PipeBuilder project(String @NonNull ... projects) throws ExprParseException {
        return new PipeBuilder(createProject(projects));
    }

    @AllArgsConstructor(access = AccessLevel.PROTECTED)
    public abstract class Builder<P extends RelOp, B extends Builder<P, B>> {
        private transient P relOp;

        protected abstract P createTandem(P op0, PipeOp op1);

        public Builder<P, B> project(Expr @NonNull ... projects) {
            relOp = createTandem(relOp, createProject(projects));
            return this;
        }

        public Builder<P, B> project(String @NonNull ... projects) throws ExprParseException {
            relOp = createTandem(relOp, createProject(projects));
            return this;
        }

        public Builder<P, B> filter(Expr filter) {
            relOp = createTandem(relOp, createFilter(filter));
            return this;
        }

        public Builder<P, B> filter(String filter) throws ExprParseException {
            relOp = createTandem(relOp, createFilter(filter));
            return this;
        }

        public P build() {
            return relOp;
        }
    }

    public final class SourceBuilder extends Builder<SourceOp, SourceBuilder> {
        private SourceBuilder(SourceOp relOp) {
            super(relOp);
        }

        @Override
        protected @NonNull SourceOp createTandem(SourceOp op0, PipeOp op1) {
            return new TandemSourceOp(op0, op1);
        }
    }

    public final class PipeBuilder extends Builder<PipeOp, PipeBuilder> {
        private PipeBuilder(PipeOp relOp) {
            super(relOp);
        }

        @Override
        protected @NonNull PipeOp createTandem(PipeOp op0, PipeOp op1) {
            return new TandemPipeOp(op0, op1);
        }
    }
}
