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

package io.dingodb.expr.parser.antlr;

import io.dingodb.expr.parser.DingoExprParser;
import io.dingodb.expr.parser.DingoExprParserBaseVisitor;
import io.dingodb.expr.parser.FunFactory;
import io.dingodb.expr.parser.exception.UndefinedFunction;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.NullaryOp;
import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.VariadicOp;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public final class DingoExprParserVisitorImpl extends DingoExprParserBaseVisitor<Expr> {
    @Getter
    private final FunFactory funFactory;

    public static @NonNull UnaryOp getUnary(int type) {
        switch (type) {
            case DingoExprParser.ADD:
                return Exprs.POS;
            case DingoExprParser.SUB:
                return Exprs.NEG;
            case DingoExprParser.NOT:
                return Exprs.NOT;
            default:
                throw new ParseCancellationException("Invalid operator type: " + type);
        }
    }

    private static @NonNull BinaryOp getBinary(int type) {
        switch (type) {
            case DingoExprParser.ADD:
                return Exprs.ADD;
            case DingoExprParser.SUB:
                return Exprs.SUB;
            case DingoExprParser.MUL:
                return Exprs.MUL;
            case DingoExprParser.DIV:
                return Exprs.DIV;
            case DingoExprParser.EQ:
                return Exprs.EQ;
            case DingoExprParser.NE:
                return Exprs.NE;
            case DingoExprParser.GT:
                return Exprs.GT;
            case DingoExprParser.GE:
                return Exprs.GE;
            case DingoExprParser.LT:
                return Exprs.LT;
            case DingoExprParser.LE:
                return Exprs.LE;
            default:
                throw new ParseCancellationException("Invalid operator type: " + type);
        }
    }

    private @NonNull Expr visitUnaryOp(int type, DingoExprParser.ExprContext expr) {
        UnaryOp op = getUnary(type);
        return Exprs.op(op, visit(expr));
    }

    private @NonNull Expr visitBinaryOp(int type, @NonNull List<DingoExprParser.ExprContext> exprList) {
        assert exprList.size() == 2;
        BinaryOp op = getBinary(type);
        return Exprs.op(op, visit(exprList.get(0)), visit(exprList.get(1)));
    }

    @Override
    public @NonNull Expr visitOr(DingoExprParser.@NonNull OrContext ctx) {
        return Exprs.op(Exprs.OR, visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public @NonNull Expr visitMulDiv(DingoExprParser.@NonNull MulDivContext ctx) {
        return visitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitAddSub(DingoExprParser.@NonNull AddSubContext ctx) {
        return visitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitVar(DingoExprParser.@NonNull VarContext ctx) {
        String name = ctx.getText();
        return Exprs.var(name);
    }

    @Override
    public @NonNull Expr visitPosNeg(DingoExprParser.@NonNull PosNegContext ctx) {
        return visitUnaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitIndex(DingoExprParser.@NonNull IndexContext ctx) {
        return Exprs.op(Exprs.INDEX, visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public @NonNull Expr visitInt(DingoExprParser.@NonNull IntContext ctx) {
        return Val.parseLiteralInt(ctx.getText());
    }

    @Override
    public @NonNull Expr visitStr(DingoExprParser.@NonNull StrContext ctx) {
        return Val.parseLiteralString(ctx.getText());
    }

    @Override
    public @NonNull Expr visitNot(DingoExprParser.@NonNull NotContext ctx) {
        return visitUnaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitRelation(DingoExprParser.@NonNull RelationContext ctx) {
        return visitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitStrIndex(DingoExprParser.@NonNull StrIndexContext ctx) {
        return Exprs.op(Exprs.INDEX, visit(ctx.expr()), ctx.ID().getText());
    }

    @Override
    public @NonNull Expr visitAnd(DingoExprParser.@NonNull AndContext ctx) {
        return Exprs.op(Exprs.AND, visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expr visitPars(DingoExprParser.@NonNull ParsContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public @NonNull Expr visitReal(DingoExprParser.@NonNull RealContext ctx) {
        return Val.parseLiteralReal(ctx.getText());
    }

    @Override
    public @NonNull Expr visitFun(DingoExprParser.@NonNull FunContext ctx) {
        String funName = ctx.fun.getText();
        int paraNum = ctx.expr().size();
        switch (paraNum) {
            case 0:
                NullaryOp nullaryFun = funFactory.getNullaryFun(funName);
                if (nullaryFun != null) {
                    return Exprs.op(nullaryFun);
                }
                break;
            case 1:
                UnaryOp unaryFun = funFactory.getUnaryFun(funName);
                if (unaryFun != null) {
                    return Exprs.op(unaryFun, visit(ctx.expr(0)));
                }
                break;
            case 2:
                BinaryOp binaryFun = funFactory.getBinaryFun(funName);
                if (binaryFun != null) {
                    return Exprs.op(binaryFun, visit(ctx.expr(0)), visit(ctx.expr(1)));
                }
                break;
            case 3:
                TertiaryOp tertiaryFun = funFactory.getTertiaryFun(funName);
                if (tertiaryFun != null) {
                    return Exprs.op(tertiaryFun, visit(ctx.expr(0)), visit(ctx.expr(1)), visit(ctx.expr(2)));
                }
                break;
            default:
                break;
        }
        VariadicOp variadicFun = funFactory.getVariadicFun(funName);
        if (variadicFun != null) {
            return Exprs.op(variadicFun, (Object[]) ctx.expr().stream().map(this::visit).toArray(Expr[]::new));
        }
        throw new UndefinedFunction(funName, paraNum);
    }
}
