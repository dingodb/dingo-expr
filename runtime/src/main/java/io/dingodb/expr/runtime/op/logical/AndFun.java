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

package io.dingodb.expr.runtime.op.logical;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.expr.VariadicOpExpr;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AndFun extends LogicalFun {
    public static final AndFun INSTANCE = new AndFun();
    public static final String NAME = "AND";

    private static final long serialVersionUID = -8938572625737275182L;

    @Override
    public AndFun getOp(Object key) {
        return (key != null && key.equals(Types.BOOL)) ? INSTANCE : null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Object eval(@NonNull Expr @NonNull [] exprs, EvalContext context, ExprConfig config) {
        Boolean result = Boolean.TRUE;
        for (Expr para : exprs) {
            Object v = para.eval(context, config);
            if (v == null) {
                if (result == Boolean.TRUE) {
                    result = null;
                }
            } else if (!(Boolean) v) {
                result = Boolean.FALSE;
                break;
            }
        }
        return result;
    }

    @Override
    public @NonNull Expr simplify(@NonNull VariadicOpExpr expr) {
        Expr[] operands = expr.getOperands();
        List<Expr> newOperands = new ArrayList<>(operands.length);
        for (Expr operand : operands) {
            if (operand instanceof Val) {
                Object v0 = operand.eval();
                if (v0 != null) {
                    if (!(Boolean) v0) {
                        return Val.FALSE;
                    }
                } else if (!newOperands.contains(Val.NULL_BOOL)) {
                    newOperands.add(Val.NULL_BOOL);
                }
            } else {
                newOperands.add(operand);
            }
        }
        switch (newOperands.size()) {
            case 0:
                return Val.TRUE;
            case 1:
                return newOperands.get(0);
            case 2:
                return Exprs.op(
                    Exprs.AND.getOp(Exprs.AND.keyOf(Types.BOOL, Types.BOOL)),
                    newOperands.get(0),
                    newOperands.get(1)
                ).simplify();
            default:
                break;
        }
        return Exprs.op(this, newOperands.toArray());
    }
}
