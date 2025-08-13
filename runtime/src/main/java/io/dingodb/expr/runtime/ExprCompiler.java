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

package io.dingodb.expr.runtime;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.compiler.CastingFactory;
import io.dingodb.expr.runtime.compiler.ConstFactory;
import io.dingodb.expr.runtime.compiler.VarFactory;
import io.dingodb.expr.runtime.compiler.VarStub;
import io.dingodb.expr.runtime.exception.ExprCompileException;
import io.dingodb.expr.runtime.exception.ExprEvaluatingException;
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.ExprVisitorBase;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.NullaryOpExpr;
import io.dingodb.expr.runtime.expr.TertiaryOpExpr;
import io.dingodb.expr.runtime.expr.UnaryOpExpr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.expr.Var;
import io.dingodb.expr.runtime.expr.VariadicOpExpr;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ExprCompiler extends ExprVisitorBase<Expr, CompileContext> {
    public static final ExprCompiler SIMPLE = new ExprCompiler(ExprConfig.SIMPLE);
    public static final ExprCompiler ADVANCED = new ExprCompiler(ExprConfig.ADVANCED);

    @Getter
    private final ExprConfig config;

    public void setExprContext(ExprContext ctx) {
        this.config.setExprContext(ctx);
    }

    public ExprContext getExprContext() {
        return this.config.getExprContext();
    }

    public static @NonNull ExprCompiler of(ExprConfig config) {
        return new ExprCompiler(config);
    }

    @Override
    public Expr visitVal(@NonNull Val expr, CompileContext obj) {
        Type type = expr.getType();
        // Do not touch non-scalar type for there's no casting for them.
        if (type.isScalar()) {
            Object value = expr.getValue();
            Type valueType = Types.valueType(value);
            if (type.matches(valueType)) {
                return expr;
            }
            return CastingFactory.get(type, config).compile(Exprs.val(value), config);
        }
        return expr;
    }

    @Override
    public Expr visitVar(@NonNull Var expr, CompileContext obj) {
        if (expr.getType() == null) {
            Object id = expr.getId();
            if (id instanceof String) {
                Val val = ConstFactory.INSTANCE.getConst((String) id);
                if (val != null) {
                    return val;
                }
            }
            if (obj != null) {
                return VarFactory.of(id, obj);
            }
            throw new ExprCompileException("Compile of vars requires a valid compiling context.");
        }
        return expr;
    }

    @Override
    public Expr visitNullaryOpExpr(@NonNull NullaryOpExpr expr, CompileContext obj) {
        return config.withSimplification() ? expr.simplify(config) : expr;
    }

    @Override
    public Expr visitUnaryOpExpr(@NonNull UnaryOpExpr expr, CompileContext obj) {
        Expr operand = visit(expr.getOperand(), obj);

        if (obj != null && obj.getExprContext() != null) {
            config.setExprContext(obj.getExprContext());
        }

        return expr.getOp().compile(operand, config);
    }

    @Override
    public Expr visitBinaryOpExpr(@NonNull BinaryOpExpr expr, CompileContext obj) {
        Expr operand0 = visit(expr.getOperand0(), obj);
        Expr operand1 = visit(expr.getOperand1(), obj);
        return expr.getOp().compile(operand0, operand1, config);
    }

    @Override
    public Expr visitTertiaryOpExpr(@NonNull TertiaryOpExpr expr, CompileContext obj) {
        Expr operand0 = visit(expr.getOperand0(), obj);
        Expr operand1 = visit(expr.getOperand1(), obj);
        Expr operand2 = visit(expr.getOperand2(), obj);
        return expr.getOp().compile(operand0, operand1, operand2, config);
    }

    @Override
    public Expr visitVariadicOpExpr(@NonNull VariadicOpExpr expr, CompileContext obj) {
        Expr[] operands = Arrays.stream(expr.getOperands())
            .map(o -> visit(o, obj))
            .toArray(Expr[]::new);
        return expr.getOp().compile(operands, config);
    }

    @Override
    public Expr visitIndexOpExpr(@NonNull IndexOpExpr expr, CompileContext obj) {
        Expr operand0 = visit(expr.getOperand0(), obj);
        Expr operand1 = visit(expr.getOperand1(), obj);
        if (operand0 instanceof VarStub) {
            try {
                Object index = operand1.eval(null, config);
                return ((VarStub) operand0).getElement(index);
            } catch (ExprEvaluatingException e) {
                throw new ExprCompileException("Not a valid var index: " + operand1);
            }
        }
        return expr.getOp().compile(operand0, operand1, config);
    }
}
