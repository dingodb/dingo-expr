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
import io.dingodb.expr.runtime.expr.UnaryOpExpr;
import io.dingodb.expr.runtime.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;

public abstract class UnaryOp extends AbstractOp<UnaryOp> {
    private static final long serialVersionUID = 4820294023360498270L;

    protected Object evalNonNullValue(@NonNull Object value, ExprConfig config) {
        throw new EvalNotImplemented(this.getClass().getCanonicalName());
    }

    public Object evalValue(Object value, ExprConfig config) {
        return (value != null) ? evalNonNullValue(value, config) : null;
    }

    public Object eval(@NonNull Expr expr, EvalContext context, ExprConfig config) {
        Object value = expr.eval(context, config);
        return evalValue(value, config);
    }

    public Object keyOf(@NonNull Type type) {
        return type;
    }

    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        return keyOf(types[0]);
    }

    public @NonNull Expr compile(@NonNull Expr operand, @NonNull ExprConfig config) {
        UnaryOpExpr result;
        Type type = operand.getType();
        UnaryOp op = getOp(keyOf(type));
        if (op != null) {
            result = Exprs.op(op, operand);
        } else {
            Type[] types = new Type[]{type};
            UnaryOp op1 = getOp(bestKeyOf(types));
            if (op1 != null) {
                result = Exprs.op(op1, doCast(operand, types[0], config));
            } else {
                throw new OperatorTypeNotExist(this, type);
            }
        }
        return config.isDoSimplification() ? result.simplify(config) : result;
    }

    public @NonNull Expr simplify(@NonNull UnaryOpExpr expr, ExprConfig ignoredConfig) {
        assert expr.getOp() == this;
        return expr;
    }

    @Override
    public UnaryOp getOp(Object key) {
        return this;
    }
}
