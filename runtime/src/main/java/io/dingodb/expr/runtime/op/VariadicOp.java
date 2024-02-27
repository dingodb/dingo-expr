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
import io.dingodb.expr.runtime.expr.VariadicOpExpr;
import io.dingodb.expr.runtime.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

public abstract class VariadicOp extends AbstractOp<VariadicOp> {
    private static final long serialVersionUID = 6629213458109720362L;

    protected Object evalNonNullValue(@NonNull Object[] value, ExprConfig config) {
        throw new EvalNotImplemented(this.getClass().getCanonicalName());
    }

    public Object evalValue(Object @NonNull [] values, ExprConfig config) {
        if (Arrays.stream(values).noneMatch(Objects::isNull)) {
            return evalNonNullValue(values, config);
        }
        return null;
    }

    public Object eval(@NonNull Expr @NonNull [] exprs, EvalContext context, ExprConfig config) {
        Object[] values = Arrays.stream(exprs).map(e -> e.eval(context, config)).toArray();
        return evalValue(values, config);
    }

    public abstract OpKey keyOf(@NonNull Type @NonNull ... types);

    public OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        return keyOf(types);
    }

    public @NonNull Expr compile(@NonNull Expr @NonNull [] operands, @NonNull ExprConfig config) {
        VariadicOpExpr result;
        Type[] types = Arrays.stream(operands).map(Expr::getType).toArray(Type[]::new);
        VariadicOp op = getOp(keyOf(types));
        if (op != null) {
            result = op.createExpr(operands);
        } else {
            VariadicOp op1 = getOp(bestKeyOf(types));
            if (op1 != null) {
                result = op1.createExpr(
                    IntStream.range(0, operands.length)
                        .mapToObj(i -> doCast(operands[i], types[i], config))
                        .toArray(Expr[]::new)
                );
            } else if (config.withGeneralOp()) {
                result = new VariadicGeneralOp(this).createExpr(operands);
            } else {
                throw new OperatorTypeNotExist(this, types);
            }
        }
        return config.withSimplification() ? result.simplify(config) : result;
    }

    public @NonNull Expr simplify(@NonNull VariadicOpExpr expr, ExprConfig config) {
        assert expr.getOp() == this;
        return expr;
    }

    @Override
    public VariadicOp getOp(OpKey key) {
        return this;
    }

    public VariadicOpExpr createExpr(@NonNull Expr @NonNull [] operands) {
        return new VariadicOpExpr(this, operands);
    }
}
