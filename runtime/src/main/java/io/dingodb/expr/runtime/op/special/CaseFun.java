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

package io.dingodb.expr.runtime.op.special;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.expr.VariadicOpExpr;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.VariadicOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class CaseFun extends VariadicOp {
    public static final CaseFun INSTANCE = new CaseFun(null);
    public static final String NAME = "CASE";

    private static final long serialVersionUID = -898834141136092315L;

    @Getter
    private final Type type;

    @Override
    public Object eval(@NonNull Expr @NonNull [] exprs, EvalContext context, ExprConfig config) {
        int size = exprs.length;
        for (int i = 0; i < size - 1; i += 2) {
            Expr expr = exprs[i];
            Object v = expr.eval(context, config);
            if (v != null && (Boolean) v) {
                return exprs[i + 1].eval(context, config);
            }
        }
        // There will be a `null` if you missed `ELSE` in SQL.
        return exprs[size - 1].eval(context, config);
    }

    @Override
    public final @Nullable OpKey keyOf(@NonNull Type @NonNull ... types) {
        Type type = null;
        int len = types.length;
        if (len % 2 != 1) {
            return null;
        }
        for (int i = 0; i < types.length - 1; i += 2) {
            if (!Types.BOOL.matches(types[i])) {
                return null;
            }
            if (type != null) {
                if (!type.matches(types[i + 1])) {
                    return null;
                }
            } else if (!types[i + 1].equals(Types.NULL)) {
                type = types[i + 1];
            }
        }
        if (type != null) {
            if (!type.matches(types[len - 1])) {
                return null;
            }
        } else {
            type = types[len - 1];
        }
        return type;
    }

    @Override
    public @NonNull Expr simplify(@NonNull VariadicOpExpr expr, ExprConfig config) {
        int size = expr.getOperands().length;
        List<Expr> nonConstExprs = new ArrayList<>(size);
        for (int i = 0; i < size - 1; i += 2) {
            Expr operand = expr.getOperands()[i];
            Expr operand1 = expr.getOperands()[i + 1];
            if (operand instanceof Val) {
                Object v = expr.eval(null, config);
                if (v != null && (Boolean) v) {
                    return operand1;
                }
            } else {
                nonConstExprs.add(operand);
                nonConstExprs.add(operand1);
            }
        }
        if (nonConstExprs.isEmpty()) {
            return expr.getOperands()[size - 1];
        }
        nonConstExprs.add(expr.getOperands()[size - 1]);
        if (nonConstExprs.size() < size) {
            return Exprs.op(expr.getOp(), (Object[]) nonConstExprs.toArray(new Expr[0]));
        }
        return expr;
    }

    @Override
    public VariadicOp getOp(OpKey key) {
        return key != null ? new CaseFun((Type) key) : null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
