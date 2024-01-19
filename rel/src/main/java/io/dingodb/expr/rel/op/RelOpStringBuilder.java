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

import io.dingodb.expr.parser.ExprParser;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.runtime.expr.Expr;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RelOpStringBuilder {
    protected final ExprParser parser;

    protected RelOpBuilder.Builder<?> realBuilder;

    private RelOpStringBuilder(ExprParser parser, RelOpBuilder.Builder<?> realBuilder) {
        this.parser = parser;
        this.realBuilder = realBuilder;
    }

    public static @NonNull InitBuilder builder(@NonNull RelConfig config) {
        return new InitBuilder(config.getExprParser());
    }

    public static @NonNull RelOpStringBuilder builder(@NonNull RelConfig config, RelOp op) {
        return new RelOpStringBuilder(config.getExprParser(), RelOpBuilder.builder(op));
    }

    public RelOp build() {
        return realBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private @NonNull <E extends Expr> List<E> parseAll(String @NonNull ... exprStrings) throws ExprParseException {
        List<E> exprs = new ArrayList<>(exprStrings.length);
        for (String exprString : exprStrings) {
            exprs.add((E) parser.parse(exprString));
        }
        return exprs;
    }

    public RelOpStringBuilder filter(String filter) throws ExprParseException {
        realBuilder = realBuilder.filter(parser.parse(filter));
        return this;
    }

    public RelOpStringBuilder project(String @NonNull ... projects) throws ExprParseException {
        realBuilder = realBuilder.project(parseAll(projects).toArray(new Expr[0]));
        return this;
    }

    public RelOpStringBuilder agg(String @NonNull ... aggList) throws ExprParseException {
        return agg(null, aggList);
    }

    public RelOpStringBuilder agg(
        int @Nullable [] groupIndices,
        String @NonNull ... aggList
    ) throws ExprParseException {
        realBuilder = realBuilder.agg(groupIndices, parseAll(aggList));
        return this;
    }

    public static class InitBuilder extends RelOpStringBuilder {
        private InitBuilder(ExprParser parser) {
            super(parser, RelOpBuilder.builder());
        }

        public RelOpStringBuilder values(List<Object @NonNull []> values) {
            RelOpBuilder.Builder<?> builder = ((RelOpBuilder.InitBuilder) realBuilder).values(values);
            return new RelOpStringBuilder(parser, builder);
        }

        public RelOpStringBuilder values(InputStream is) {
            RelOpBuilder.Builder<?> builder = ((RelOpBuilder.InitBuilder) realBuilder).values(is);
            return new RelOpStringBuilder(parser, builder);
        }

        public RelOpStringBuilder values(String... csvLines) {
            RelOpBuilder.Builder<?> builder = ((RelOpBuilder.InitBuilder) realBuilder).values(csvLines);
            return new RelOpStringBuilder(parser, builder);
        }
    }
}
