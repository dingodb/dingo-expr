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
import io.dingodb.expr.runtime.op.OpSymbol;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.op.VariadicOp;
import io.dingodb.expr.runtime.type.Type;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.stream.Collectors;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode(of = {"op", "operands"})
public final class VariadicOpExpr implements OpExpr {
    private static final long serialVersionUID = -2145574267641248415L;

    @Getter
    private final VariadicOp op;
    @Getter
    private final Expr[] operands;

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        return op.eval(operands, context, config);
    }

    @Override
    public Type getType() {
        return op.getType();
    }

    @Override
    public @NonNull Expr simplify(ExprConfig config) {
        if (Arrays.stream(operands).allMatch(o -> o instanceof Val)) {
            return Exprs.val(eval(null, config), getType());
        }
        return op.simplify(this, config);
    }

    @Override
    public <R, T> R accept(@NonNull ExprVisitor<R, T> visitor, T obj) {
        return visitor.visitVariadicOpExpr(this, obj);
    }

    @Override
    public @NonNull String toString() {
        OpType opType = op.getOpType();
        assert opType.getSymbol().equals(OpSymbol.FUN);
        return op.getName() + "(" + Arrays.stream(operands)
            .map(Expr::toString)
            .collect(Collectors.joining(", ")) + ")";
    }
}
