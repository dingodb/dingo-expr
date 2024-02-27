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

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.exception.EvalNotImplemented;
import io.dingodb.expr.runtime.exception.OperatorTypeNotExist;
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class BinaryOp extends AbstractOp<BinaryOp> {
    private static final long serialVersionUID = -3432586934529603722L;

    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        throw new EvalNotImplemented(this.getClass().getCanonicalName());
    }

    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        return (value0 != null && value1 != null) ? evalNonNullValue(value0, value1, config) : null;
    }

    public Object eval(@NonNull Expr expr0, @NonNull Expr expr1, EvalContext context, ExprConfig config) {
        Object value0 = expr0.eval(context, config);
        Object value1 = expr1.eval(context, config);
        return evalValue(value0, value1, config);
    }

    public @Nullable OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return OpKeys.DEFAULT.keyOf(type0, type1);
    }

    public OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        return keyOf(types[0], types[1]);
    }

    public @NonNull Expr compile(@NonNull Expr operand0, @NonNull Expr operand1, @NonNull ExprConfig config) {
        BinaryOpExpr result;
        Type type0 = operand0.getType();
        Type type1 = operand1.getType();
        BinaryOp op = getOp(keyOf(type0, type1));
        if (op != null) {
            result = op.createExpr(operand0, operand1);
        } else {
            Type[] types = new Type[]{type0, type1};
            BinaryOp op1 = getOp(bestKeyOf(types));
            if (op1 != null) {
                result = op1.createExpr(doCast(operand0, types[0], config), doCast(operand1, types[1], config));
            } else if (config.withGeneralOp()) {
                result = new BinaryGeneralOp(this).createExpr(operand0, operand1);
            } else {
                throw new OperatorTypeNotExist(this, type0, type1);
            }
        }
        return config.withSimplification() ? result.simplify(config) : result;
    }

    public @NonNull Expr simplify(@NonNull BinaryOpExpr expr, ExprConfig config) {
        assert expr.getOp() == this;
        return expr;
    }

    @Override
    public BinaryOp getOp(OpKey key) {
        return this;
    }

    public BinaryOpExpr createExpr(@NonNull Expr operand0, @NonNull Expr operand1) {
        return new BinaryOpExpr(this, operand0, operand1);
    }
}
