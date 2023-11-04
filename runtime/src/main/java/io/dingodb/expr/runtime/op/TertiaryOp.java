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
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.TertiaryOpExpr;
import io.dingodb.expr.runtime.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;

public abstract class TertiaryOp extends AbstractOp<TertiaryOp> {
    private static final long serialVersionUID = 7024305609460207841L;

    protected Object evalNonNullValue(
        @NonNull Object value0,
        @NonNull Object value1,
        @NonNull Object value2,
        ExprConfig config
    ) {
        throw new EvalNotImplemented(this.getClass().getCanonicalName());
    }

    protected Object evalValue(
        Object value0,
        Object value1,
        Object value2,
        ExprConfig config
    ) {
        return (value0 != null && value1 != null) ? evalNonNullValue(value0, value1, value2, config) : null;
    }

    public Object eval(
        @NonNull Expr expr0,
        @NonNull Expr expr1,
        @NonNull Expr expr2,
        EvalContext context,
        ExprConfig config
    ) {
        Object value0 = expr0.eval(context, config);
        Object value1 = expr1.eval(context, config);
        Object value2 = expr2.eval(context, config);
        return evalValue(value0, value1, value2, config);
    }

    public abstract Object keyOf(@NonNull Type type0, @NonNull Type type1, @NonNull Type type2);

    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        return keyOf(types[0], types[1], types[2]);
    }

    public @NonNull Expr compile(
        @NonNull Expr operand0,
        @NonNull Expr operand1,
        @NonNull Expr operand2,
        @NonNull ExprConfig config
    ) {
        TertiaryOpExpr result;
        Type type0 = operand0.getType();
        Type type1 = operand1.getType();
        Type type2 = operand1.getType();
        TertiaryOp op = getOp(keyOf(type0, type1, type2));
        if (op != null) {
            result = Exprs.op(op, operand0, operand1, operand2);
        } else {
            Type[] types = new Type[]{type0, type1, type2};
            TertiaryOp op1 = getOp(bestKeyOf(types));
            if (op1 != null) {
                result = Exprs.op(
                    op1,
                    doCast(operand0, types[0], config),
                    doCast(operand1, types[1], config),
                    doCast(operand2, types[2], config)
                );
            } else {
                throw new OperatorTypeNotExist(this, type0, type1);
            }
        }
        return config.isDoSimplification() ? result.simplify() : result;
    }

    public @NonNull Expr simplify(@NonNull TertiaryOpExpr expr) {
        assert expr.getOp() == this;
        return expr;
    }

    @Override
    public TertiaryOp getOp(Object key) {
        return this;
    }
}
