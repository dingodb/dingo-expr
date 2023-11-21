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
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.type.Type;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode(of = {"op", "operand0", "operand1"})
public class TertiaryOpExpr implements OpExpr {
    private static final long serialVersionUID = -1597326280376061590L;

    @Getter
    protected final TertiaryOp op;
    @Getter
    protected final Expr operand0;
    @Getter
    protected final Expr operand1;
    @Getter
    protected final Expr operand2;

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        return op.eval(operand0, operand1, operand2, context, config);
    }

    @Override
    public Type getType() {
        return op.getType();
    }

    @Override
    public @NonNull Expr simplify(ExprConfig config) {
        if (operand0 instanceof Val && operand1 instanceof Val && operand2 instanceof Val) {
            return Exprs.val(eval(null, config), getType());
        }
        return op.simplify(this, config);
    }

    @Override
    public <R, T> R accept(@NonNull ExprVisitor<R, T> visitor, T obj) {
        return visitor.visitTertiaryOpExpr(this, obj);
    }

    @Override
    public @NonNull OpType getOpType() {
        return op.getOpType();
    }

    @Override
    public String toString() {
        OpType opType = op.getOpType();
        assert opType == OpType.FUN;
        return op.getName() + "(" + operand0 + ", " + operand1 + ", " + operand2 + ")";
    }
}
