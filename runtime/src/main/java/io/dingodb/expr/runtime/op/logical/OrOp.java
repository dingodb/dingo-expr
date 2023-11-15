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
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.op.OpType;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class OrOp extends LogicalOp {
    public static final OrOp INSTANCE = new OrOp();

    private static final long serialVersionUID = -2362749171713617181L;

    @Override
    public Object eval(@NonNull Expr expr0, @NonNull Expr expr1, EvalContext context, ExprConfig config) {
        Boolean result = Boolean.FALSE;
        Object v0 = expr0.eval(context, config);
        if (v0 == null) {
            result = null;
        } else if ((Boolean) v0) {
            return true;
        }
        Object v1 = expr1.eval(context, config);
        if (v1 == null) {
            if (result == Boolean.FALSE) {
                return null;
            }
        } else if ((Boolean) v1) {
            return true;
        }
        return result;
    }

    @Override
    public @NonNull Expr simplify(@NonNull BinaryOpExpr expr) {
        Expr operand0 = expr.getOperand0();
        Expr operand1 = expr.getOperand1();
        // Both 2 operands are Val is processed in BinaryOpExpr.simplify.
        if (operand0 instanceof Val) {
            Object v0 = operand0.eval();
            if (v0 != null) {
                if ((Boolean) v0) {
                    return Val.TRUE;
                } else {
                    return operand1;
                }
            } else {
                return Exprs.op(this, Val.NULL_BOOL, operand1);
            }
        }
        if (operand1 instanceof Val) {
            Object v1 = operand1.eval();
            if (v1 != null) {
                if ((Boolean) v1) {
                    return Val.TRUE;
                } else {
                    return operand0;
                }
            } else {
                return Exprs.op(this, operand0, Val.NULL_BOOL);
            }
        }
        return super.simplify(expr);
    }

    @Override
    public @NonNull OpType getOpType() {
        return OpType.OR;
    }
}
