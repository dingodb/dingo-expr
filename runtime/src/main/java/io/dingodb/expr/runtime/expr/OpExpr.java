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

import io.dingodb.expr.runtime.op.AbstractOp;
import io.dingodb.expr.runtime.op.OpType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

@EqualsAndHashCode(of = {"op"})
public abstract class OpExpr<O extends AbstractOp<O, E>, E extends OpExpr<O, E>> implements Expr {
    private static final long serialVersionUID = -9011805373573763769L;

    @Getter
    protected final O op;

    protected OpExpr(O op) {
        this.op = op;
    }

    public @NonNull OpType getOpType() {
        return getOp().getOpType();
    }

    protected String oprandToString(Expr operand) {
        if (operand instanceof OpExpr) {
            OpExpr<?, ?> op = (OpExpr<?, ?>) operand;
            if (op.getOpType().getPrecedence() > getOpType().getPrecedence()) {
                return "(" + operand + ")";
            }
        }
        return operand.toString();
    }
}
