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
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.TupleCompileContext;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.TupleType;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AggregateOp implements CacheOp {
    @Getter
    protected final List<Expr> aggList;

    @Getter
    protected TupleType type;

    protected transient ExprConfig exprConfig;

    @Override
    public void init(TupleType type, @NonNull RelConfig config) {
        exprConfig = config.getExprCompiler().getConfig();
        final TupleCompileContext context = new TupleCompileContext(type);
        ExprCompiler compiler = config.getExprCompiler();
        aggList.replaceAll(agg -> compiler.visit(agg, context));
    }
}
