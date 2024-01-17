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

package io.dingodb.expr.runtime.op;

import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.exception.FailEvaluatingValues;
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.type.Type;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class BinaryGeneralOp extends BinaryOp {
    private static final long serialVersionUID = -664270463407354496L;

    private final BinaryOp op;

    @Override
    public Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        return null;
    }

    @Override
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        Expr expr = ExprCompiler.SIMPLE.visit(Exprs.op(op, value0, value1));
        if (expr instanceof BinaryOpExpr && ((BinaryOpExpr) expr).getOp().getKey() == null) {
            throw new FailEvaluatingValues(this, value0, value1);
        }
        return expr.eval(null, config);
    }

    @Override
    public Type getType() {
        return op.getType();
    }

    @Override
    public @NonNull String getName() {
        return op.getName();
    }
}
