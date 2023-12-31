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
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.op.OpType;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AndOp extends LogicalOp {
    public static final AndOp INSTANCE = new AndOp();

    private static final long serialVersionUID = -8663736343177194386L;

    @Override
    public Object eval(@NonNull Expr expr0, @NonNull Expr expr1, EvalContext context, ExprConfig config) {
        Boolean result = Boolean.TRUE;
        Object v0 = expr0.eval(context, config);
        if (v0 == null) {
            result = null;
        } else if (!(Boolean) v0) {
            return false;
        }
        Object v1 = expr1.eval(context, config);
        if (v1 == null) {
            if (result == Boolean.TRUE) {
                return null;
            }
        } else if (!(Boolean) v1) {
            return false;
        }
        return result;
    }

    @Override
    public @NonNull Expr simplify(@NonNull BinaryOpExpr expr, ExprConfig config) {
        Expr operand0 = expr.getOperand0();
        Expr operand1 = expr.getOperand1();
        // Both 2 operands are Val is processed in BinaryOpExpr.simplify.
        if (operand0 instanceof Val) {
            Object v0 = operand0.eval(null, config);
            if (v0 != null) {
                if ((Boolean) v0) {
                    return operand1;
                } else {
                    return Val.FALSE;
                }
            } else {
                return createExpr(Val.NULL_BOOL, operand1);
            }
        }
        if (operand1 instanceof Val) {
            Object v1 = operand1.eval(null, config);
            if (v1 != null) {
                if ((Boolean) v1) {
                    return operand0;
                } else {
                    return Val.FALSE;
                }
            } else {
                return createExpr(operand0, Val.NULL_BOOL);
            }
        }
        return expr;
    }

    @Override
    public @NonNull OpType getOpType() {
        return OpType.AND;
    }
}
