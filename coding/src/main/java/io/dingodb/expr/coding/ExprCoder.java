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

package io.dingodb.expr.coding;

import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.ExprVisitorBase;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.NullaryOpExpr;
import io.dingodb.expr.runtime.expr.TertiaryOpExpr;
import io.dingodb.expr.runtime.expr.UnaryOpExpr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.expr.Var;
import io.dingodb.expr.runtime.expr.VariadicOpExpr;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.op.logical.AndFun;
import io.dingodb.expr.runtime.op.logical.OrFun;
import io.dingodb.expr.runtime.op.mathematical.AbsFunFactory;
import io.dingodb.expr.runtime.op.mathematical.MaxFunFactory;
import io.dingodb.expr.runtime.op.mathematical.MinFunFactory;
import io.dingodb.expr.runtime.op.mathematical.ModFunFactory;
import io.dingodb.expr.runtime.op.special.IsFalseFunFactory;
import io.dingodb.expr.runtime.op.special.IsNullFunFactory;
import io.dingodb.expr.runtime.op.special.IsTrueFunFactory;
import io.dingodb.expr.runtime.type.BoolType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.expr.runtime.type.LongType;
import io.dingodb.expr.runtime.type.StringType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.TypeVisitorBase;
import io.dingodb.expr.runtime.utils.CodecUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ExprCoder extends ExprVisitorBase<ExprCoder.CodingFlag, @NonNull OutputStream> {
    public static final ExprCoder INSTANCE = new ExprCoder();
    public static final CodingFlag OK = new CodingFlag();

    private static final byte NULL = (byte) 0x00;
    private static final byte CONST = (byte) 0x10;
    private static final byte CONST_N = (byte) 0x20;
    private static final byte VAR_I = (byte) 0x30;

    private static final byte POS = (byte) 0x81;
    private static final byte NEG = (byte) 0x82;
    private static final byte ADD = (byte) 0x83;
    private static final byte SUB = (byte) 0x84;
    private static final byte MUL = (byte) 0x85;
    private static final byte DIV = (byte) 0x86;
    private static final byte MOD = (byte) 0x87;

    private static final byte EQ = (byte) 0x91;
    private static final byte GE = (byte) 0x92;
    private static final byte GT = (byte) 0x93;
    private static final byte LE = (byte) 0x94;
    private static final byte LT = (byte) 0x95;
    private static final byte NE = (byte) 0x96;

    private static final byte NOT = (byte) 0x51;
    private static final byte AND = (byte) 0x52;
    private static final byte OR = (byte) 0x53;

    private static final byte IS_NULL = (byte) 0xA1;
    private static final byte IS_TRUE = (byte) 0xA2;
    private static final byte IS_FALSE = (byte) 0xA3;

    private static final byte MIN = (byte) 0xB1;
    private static final byte MAX = (byte) 0xB2;
    private static final byte ABS = (byte) 0xB3;

    private static final byte CAST = (byte) 0xF0;

    private static final byte FUN = (byte) 0xF1;

    private static boolean writeOpWithType(OutputStream obj, byte opByte, Type type) throws IOException {
        Byte typeByte;
        typeByte = TypeCoder.INSTANCE.visit(type);
        if (typeByte != null) {
            obj.write(opByte);
            obj.write(typeByte);
            return true;
        }
        return false;
    }

    private static boolean writeFun(OutputStream os, int funIndex) throws IOException {
        if (funIndex > 0) {
            os.write(FUN);
            os.write(funIndex);
            return true;
        }
        return funIndex == 0;
    }

    private boolean cascadingBinaryLogical(OutputStream os, byte opByte, Expr @NonNull [] operands) throws IOException {
        if (visit(operands[0], os) != OK) {
            return false;
        }
        for (int i = 1; i < operands.length; ++i) {
            if (visit(operands[i], os) != OK) {
                return false;
            }
            os.write(opByte);
        }
        return true;
    }

    @Override
    public CodingFlag visitVal(@NonNull Val expr, OutputStream obj) {
        return new ValCoder(expr).visit(expr.getType(), obj);
    }

    @SneakyThrows
    @Override
    public CodingFlag visitVar(@NonNull Var expr, OutputStream obj) {
        Object id = expr.getId();
        if (id instanceof Integer) {
            if ((Integer) id >= 0) {
                Byte typeByte = TypeCoder.INSTANCE.visit(expr.getType());
                if (typeByte != null) {
                    obj.write(VAR_I | TypeCoder.INSTANCE.visit(expr.getType()));
                    CodecUtils.encodeVarInt(obj, (Integer) id);
                    return OK;
                }
            }
            // TODO: SQL parameters are not supported currently.
        }
        return null;
    }

    @Override
    public CodingFlag visitNullaryOpExpr(@NonNull NullaryOpExpr expr, @NonNull OutputStream obj) {
        return super.visitNullaryOpExpr(expr, obj);
    }

    @SneakyThrows
    @Override
    public CodingFlag visitUnaryOpExpr(@NonNull UnaryOpExpr expr, OutputStream obj) {
        if (visit(expr.getOperand(), obj) == OK) {
            boolean success = false;
            switch (expr.getOpType()) {
                case POS:
                    success = writeOpWithType(obj, POS, (Type) expr.getOp().getKey());
                    break;
                case NEG:
                    success = writeOpWithType(obj, NEG, (Type) expr.getOp().getKey());
                    break;
                case NOT:
                    obj.write(NOT);
                    success = true;
                    break;
                case CAST:
                    Byte dstType = TypeCoder.INSTANCE.visit(expr.getType());
                    Byte srcType = TypeCoder.INSTANCE.visit((Type) expr.getOp().getKey());
                    if (dstType != null && srcType != null) {
                        obj.write(CAST);
                        obj.write(dstType << 4 | srcType);
                        success = true;
                    }
                    break;
                case FUN:
                    switch (expr.getOp().getName()) {
                        case IsNullFunFactory.NAME:
                            success = writeOpWithType(obj, IS_NULL, (Type) expr.getOp().getKey());
                            break;
                        case IsTrueFunFactory.NAME:
                            success = writeOpWithType(obj, IS_TRUE, (Type) expr.getOp().getKey());
                            break;
                        case IsFalseFunFactory.NAME:
                            success = writeOpWithType(obj, IS_FALSE, (Type) expr.getOp().getKey());
                            break;
                        case AbsFunFactory.NAME:
                            success = writeOpWithType(obj, ABS, (Type) expr.getOp().getKey());
                            break;
                        default:
                            success = writeFun(obj, FunIndex.getUnary(expr.getOp()));
                            break;
                    }
                    break;
                default:
                    break;
            }
            if (success) {
                return OK;
            }
        }
        return null;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitBinaryOpExpr(@NonNull BinaryOpExpr expr, OutputStream obj) {
        if (visit(expr.getOperand0(), obj) == OK && visit(expr.getOperand1(), obj) == OK) {
            boolean success = false;
            switch (expr.getOpType()) {
                case ADD:
                    success = writeOpWithType(obj, ADD, (Type) expr.getOp().getKey());
                    break;
                case SUB:
                    success = writeOpWithType(obj, SUB, (Type) expr.getOp().getKey());
                    break;
                case MUL:
                    success = writeOpWithType(obj, MUL, (Type) expr.getOp().getKey());
                    break;
                case DIV:
                    success = writeOpWithType(obj, DIV, (Type) expr.getOp().getKey());
                    break;
                case EQ:
                    success = writeOpWithType(obj, EQ, (Type) expr.getOp().getKey());
                    break;
                case NE:
                    success = writeOpWithType(obj, NE, (Type) expr.getOp().getKey());
                    break;
                case GT:
                    success = writeOpWithType(obj, GT, (Type) expr.getOp().getKey());
                    break;
                case GE:
                    success = writeOpWithType(obj, GE, (Type) expr.getOp().getKey());
                    break;
                case LT:
                    success = writeOpWithType(obj, LT, (Type) expr.getOp().getKey());
                    break;
                case LE:
                    success = writeOpWithType(obj, LE, (Type) expr.getOp().getKey());
                    break;
                case AND:
                    obj.write(AND);
                    success = true;
                    break;
                case OR:
                    obj.write(OR);
                    success = true;
                    break;
                case FUN:
                    switch (expr.getOp().getName()) {
                        case MinFunFactory.NAME:
                            success = writeOpWithType(obj, MIN, (Type) expr.getOp().getKey());
                            break;
                        case MaxFunFactory.NAME:
                            success = writeOpWithType(obj, MAX, (Type) expr.getOp().getKey());
                            break;
                        case ModFunFactory.NAME:
                            success = writeOpWithType(obj, MOD, (Type) expr.getOp().getKey());
                            break;
                        default:
                            success = writeFun(obj, FunIndex.getBinary(expr.getOp()));
                            break;
                    }
                    break;
                default:
                    break;
            }
            if (success) {
                return OK;
            }
        }
        return null;
    }

    @Override
    public CodingFlag visitTertiaryOpExpr(@NonNull TertiaryOpExpr expr, @NonNull OutputStream obj) {
        return super.visitTertiaryOpExpr(expr, obj);
    }

    @SneakyThrows
    @Override
    public CodingFlag visitVariadicOpExpr(@NonNull VariadicOpExpr expr, OutputStream obj) {
        if (expr.getOpType() == OpType.FUN) {
            boolean success = false;
            switch (expr.getOp().getName()) {
                case AndFun.NAME:
                    success = cascadingBinaryLogical(obj, AND, expr.getOperands());
                    break;
                case OrFun.NAME:
                    success = cascadingBinaryLogical(obj, OR, expr.getOperands());
                    break;
                default:
                    break;
            }
            if (success) {
                return OK;
            }
        }
        return null;
    }

    @Override
    public CodingFlag visitIndexOpExpr(@NonNull IndexOpExpr expr, OutputStream obj) {
        return null;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class ValCoder extends TypeVisitorBase<CodingFlag, OutputStream> {
        private final Val val;

        @SneakyThrows
        @Override
        public CodingFlag visitIntType(@NonNull IntType type, OutputStream obj) {
            Integer value = (Integer) val.getValue();
            if (value != null) {
                if (value >= 0) {
                    obj.write(CONST | TypeCoder.TYPE_INT32);
                    CodecUtils.encodeVarInt(obj, value);
                } else {
                    obj.write(CONST_N | TypeCoder.TYPE_INT32);
                    CodecUtils.encodeVarInt(obj, -(long) value); // value may overflow for `Integer.MIN_VALUE`.
                }
            } else {
                obj.write(NULL | TypeCoder.TYPE_INT32);
            }
            return OK;
        }

        @SneakyThrows
        @Override
        public CodingFlag visitLongType(@NonNull LongType type, OutputStream obj) {
            Long value = (Long) val.getValue();
            if (value != null) {
                if (value >= 0 || value == Long.MIN_VALUE) {
                    obj.write(CONST | TypeCoder.TYPE_INT64);
                    CodecUtils.encodeVarInt(obj, value);
                } else {
                    obj.write(CONST_N | TypeCoder.TYPE_INT64);
                    CodecUtils.encodeVarInt(obj, -value);
                }
            } else {
                obj.write(NULL | TypeCoder.TYPE_INT64);
            }
            return OK;
        }

        @SneakyThrows
        @Override
        public CodingFlag visitFloatType(@NonNull FloatType type, OutputStream obj) {
            Float value = (Float) val.getValue();
            if (value != null) {
                obj.write(CONST | TypeCoder.TYPE_FLOAT);
                CodecUtils.encodeFloat(obj, value);
            } else {
                obj.write(NULL | TypeCoder.TYPE_FLOAT);
            }
            return OK;
        }

        @SneakyThrows
        @Override
        public CodingFlag visitDoubleType(@NonNull DoubleType type, OutputStream obj) {
            Double value = (Double) val.getValue();
            if (value != null) {
                obj.write(CONST | TypeCoder.TYPE_DOUBLE);
                CodecUtils.encodeDouble(obj, value);
            } else {
                obj.write(NULL | TypeCoder.TYPE_DOUBLE);
            }
            return OK;
        }

        @SneakyThrows
        @Override
        public CodingFlag visitBoolType(@NonNull BoolType type, OutputStream obj) {
            Boolean value = (Boolean) val.getValue();
            if (value != null) {
                obj.write(value ? CONST | TypeCoder.TYPE_BOOL : CONST_N | TypeCoder.TYPE_BOOL);
            } else {
                obj.write(NULL | TypeCoder.TYPE_BOOL);
            }
            return OK;
        }

        @SneakyThrows
        @Override
        public CodingFlag visitStringType(@NonNull StringType type, OutputStream obj) {
            String value = (String) val.getValue();
            if (value != null) {
                obj.write(CONST | TypeCoder.TYPE_STRING);
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                CodecUtils.encodeVarInt(obj, bytes.length);
                obj.write(bytes);
            } else {
                obj.write(NULL | TypeCoder.TYPE_STRING);
            }
            return OK;
        }
    }

    public static final class CodingFlag {
        private CodingFlag() {
        }
    }
}
