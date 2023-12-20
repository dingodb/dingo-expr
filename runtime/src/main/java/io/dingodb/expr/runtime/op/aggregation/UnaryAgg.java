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

package io.dingodb.expr.runtime.op.aggregation;

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.UnaryAggExpr;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class UnaryAgg extends UnaryOp implements Agg {
    private static final long serialVersionUID = -1765824099692470414L;

    /**
     * Add a new value to aggregating context.
     *
     * @param var   the current value from the aggregating context, {@code null} means empty
     * @param value the new value
     * @return the result value
     */
    public abstract Object add(@Nullable Object var, @Nullable Object value, ExprConfig config);

    @Override
    public UnaryAggExpr createExpr(@NonNull Expr operand) {
        return new UnaryAggExpr(this, operand);
    }
}
