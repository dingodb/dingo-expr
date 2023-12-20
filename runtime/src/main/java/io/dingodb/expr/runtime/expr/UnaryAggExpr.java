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

package io.dingodb.expr.runtime.expr;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.aggregation.UnaryAgg;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class UnaryAggExpr extends UnaryOpExpr implements AggExpr {
    private static final long serialVersionUID = -382480126048094131L;

    public UnaryAggExpr(UnaryAgg op, Expr operand) {
        super(op, operand);
    }

    @Override
    public Object add(@Nullable Object var, @NonNull EvalContext rowContext, ExprConfig config) {
        Object value = operand.eval(rowContext, config);
        return ((UnaryAgg) op).add(var, value, config);
    }

    @Override
    public Object merge(@Nullable Object var1, @Nullable Object var2, ExprConfig config) {
        if (var1 != null) {
            if (var2 != null) {
                return ((UnaryAgg) op).merge(var1, var2, config);
            }
            return var1;
        }
        return var2;
    }

    @Override
    public Object emptyValue() {
        return ((UnaryAgg) op).emptyValue();
    }

    @Override
    public <R, T> R accept(@NonNull ExprVisitor<R, T> visitor, T obj) {
        return visitor.visitUnaryAggExpr(this, obj);
    }
}
