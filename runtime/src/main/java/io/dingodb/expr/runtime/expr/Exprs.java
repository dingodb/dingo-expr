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

import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.NullaryOp;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.VariadicOp;
import io.dingodb.expr.runtime.op.arithmetic.AddOpFactory;
import io.dingodb.expr.runtime.op.arithmetic.DivOpFactory;
import io.dingodb.expr.runtime.op.arithmetic.MulOpFactory;
import io.dingodb.expr.runtime.op.arithmetic.NegOpFactory;
import io.dingodb.expr.runtime.op.arithmetic.PosOpFactory;
import io.dingodb.expr.runtime.op.arithmetic.SubOpFactory;
import io.dingodb.expr.runtime.op.cast.BoolCastOpFactory;
import io.dingodb.expr.runtime.op.cast.BytesCastOpFactory;
import io.dingodb.expr.runtime.op.cast.DateCastOpFactory;
import io.dingodb.expr.runtime.op.cast.DecimalCastOpFactory;
import io.dingodb.expr.runtime.op.cast.DoubleCastOpFactory;
import io.dingodb.expr.runtime.op.cast.FloatCastOpFactory;
import io.dingodb.expr.runtime.op.cast.IntCastCheckOpFactory;
import io.dingodb.expr.runtime.op.cast.IntCastOpFactory;
import io.dingodb.expr.runtime.op.cast.LongCastCheckOpFactory;
import io.dingodb.expr.runtime.op.cast.LongCastOpFactory;
import io.dingodb.expr.runtime.op.cast.StringCastOpFactory;
import io.dingodb.expr.runtime.op.cast.TimeCastOpFactory;
import io.dingodb.expr.runtime.op.cast.TimestampCastOpFactory;
import io.dingodb.expr.runtime.op.collection.ArrayConstructorOpFactory;
import io.dingodb.expr.runtime.op.collection.ListConstructorOpFactory;
import io.dingodb.expr.runtime.op.index.IndexOp;
import io.dingodb.expr.runtime.op.logical.AndFun;
import io.dingodb.expr.runtime.op.logical.AndOp;
import io.dingodb.expr.runtime.op.logical.NotOpFactory;
import io.dingodb.expr.runtime.op.logical.OrFun;
import io.dingodb.expr.runtime.op.logical.OrOp;
import io.dingodb.expr.runtime.op.mathematical.AbsFunFactory;
import io.dingodb.expr.runtime.op.mathematical.AcosFunFactory;
import io.dingodb.expr.runtime.op.mathematical.AsinFunFactory;
import io.dingodb.expr.runtime.op.mathematical.AtanFunFactory;
import io.dingodb.expr.runtime.op.mathematical.CeilFunFactory;
import io.dingodb.expr.runtime.op.mathematical.CosFunFactory;
import io.dingodb.expr.runtime.op.mathematical.CoshFunFactory;
import io.dingodb.expr.runtime.op.mathematical.ExpFunFactory;
import io.dingodb.expr.runtime.op.mathematical.FloorFunFactory;
import io.dingodb.expr.runtime.op.mathematical.LogFunFactory;
import io.dingodb.expr.runtime.op.mathematical.MaxFunFactory;
import io.dingodb.expr.runtime.op.mathematical.MinFunFactory;
import io.dingodb.expr.runtime.op.mathematical.ModFunFactory;
import io.dingodb.expr.runtime.op.mathematical.SinFunFactory;
import io.dingodb.expr.runtime.op.mathematical.SinhFunFactory;
import io.dingodb.expr.runtime.op.mathematical.TanFunFactory;
import io.dingodb.expr.runtime.op.mathematical.TanhFunFactory;
import io.dingodb.expr.runtime.op.relational.EqOpFactory;
import io.dingodb.expr.runtime.op.relational.GeOpFactory;
import io.dingodb.expr.runtime.op.relational.GtOpFactory;
import io.dingodb.expr.runtime.op.relational.LeOpFactory;
import io.dingodb.expr.runtime.op.relational.LtOpFactory;
import io.dingodb.expr.runtime.op.relational.NeOpFactory;
import io.dingodb.expr.runtime.op.special.IsFalseFunFactory;
import io.dingodb.expr.runtime.op.special.IsNullFunFactory;
import io.dingodb.expr.runtime.op.special.IsTrueFunFactory;
import io.dingodb.expr.runtime.op.string.CharLengthFunFactory;
import io.dingodb.expr.runtime.op.string.ConcatFunFactory;
import io.dingodb.expr.runtime.op.string.ConvertTimeFormatFunFactory;
import io.dingodb.expr.runtime.op.string.HexFunFactory;
import io.dingodb.expr.runtime.op.string.LTrimFunFactory;
import io.dingodb.expr.runtime.op.string.LeftFunFactory;
import io.dingodb.expr.runtime.op.string.Locate2FunFactory;
import io.dingodb.expr.runtime.op.string.Locate3FunFactory;
import io.dingodb.expr.runtime.op.string.LowerFunFactory;
import io.dingodb.expr.runtime.op.string.Mid2FunFactory;
import io.dingodb.expr.runtime.op.string.Mid3FunFactory;
import io.dingodb.expr.runtime.op.string.RTrimFunFactory;
import io.dingodb.expr.runtime.op.string.RepeatFunFactory;
import io.dingodb.expr.runtime.op.string.ReplaceFunFactory;
import io.dingodb.expr.runtime.op.string.ReverseFunFactory;
import io.dingodb.expr.runtime.op.string.RightFunFactory;
import io.dingodb.expr.runtime.op.string.Substr2FunFactory;
import io.dingodb.expr.runtime.op.string.Substr3FunFactory;
import io.dingodb.expr.runtime.op.string.TrimFunFactory;
import io.dingodb.expr.runtime.op.string.UpperFunFactory;
import io.dingodb.expr.runtime.op.time.CurrentDateFun;
import io.dingodb.expr.runtime.op.time.CurrentTimeFun;
import io.dingodb.expr.runtime.op.time.CurrentTimestampFun;
import io.dingodb.expr.runtime.op.time.DateFormat1FunFactory;
import io.dingodb.expr.runtime.op.time.DateFormat2FunFactory;
import io.dingodb.expr.runtime.op.time.TimeFormat1FunFactory;
import io.dingodb.expr.runtime.op.time.TimeFormat2FunFactory;
import io.dingodb.expr.runtime.op.time.TimestampFormat1FunFactory;
import io.dingodb.expr.runtime.op.time.TimestampFormat2FunFactory;
import io.dingodb.expr.runtime.type.AnyType;
import io.dingodb.expr.runtime.type.ArrayType;
import io.dingodb.expr.runtime.type.BoolType;
import io.dingodb.expr.runtime.type.BytesType;
import io.dingodb.expr.runtime.type.DateType;
import io.dingodb.expr.runtime.type.DecimalType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.LongType;
import io.dingodb.expr.runtime.type.MapType;
import io.dingodb.expr.runtime.type.NullType;
import io.dingodb.expr.runtime.type.StringType;
import io.dingodb.expr.runtime.type.TimeType;
import io.dingodb.expr.runtime.type.TimestampType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.TypeVisitorBase;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

public final class Exprs {
    // Castings
    public static final IntCastOpFactory TO_INT = IntCastOpFactory.INSTANCE;
    public static final IntCastCheckOpFactory TO_INT_C = IntCastCheckOpFactory.INSTANCE;
    public static final LongCastOpFactory TO_LONG = LongCastOpFactory.INSTANCE;
    public static final LongCastCheckOpFactory TO_LONG_C = LongCastCheckOpFactory.INSTANCE;
    public static final FloatCastOpFactory TO_FLOAT = FloatCastOpFactory.INSTANCE;
    public static final DoubleCastOpFactory TO_DOUBLE = DoubleCastOpFactory.INSTANCE;
    public static final BoolCastOpFactory TO_BOOL = BoolCastOpFactory.INSTANCE;
    public static final DecimalCastOpFactory TO_DECIMAL = DecimalCastOpFactory.INSTANCE;
    public static final StringCastOpFactory TO_STRING = StringCastOpFactory.INSTANCE;
    public static final BytesCastOpFactory TO_BYTES = BytesCastOpFactory.INSTANCE;
    public static final DateCastOpFactory TO_DATE = DateCastOpFactory.INSTANCE;
    public static final TimeCastOpFactory TO_TIME = TimeCastOpFactory.INSTANCE;
    public static final TimestampCastOpFactory TO_TIMESTAMP = TimestampCastOpFactory.INSTANCE;

    // Arithmetics
    public static final PosOpFactory POS = PosOpFactory.INSTANCE;
    public static final NegOpFactory NEG = NegOpFactory.INSTANCE;
    public static final AddOpFactory ADD = AddOpFactory.INSTANCE;
    public static final SubOpFactory SUB = SubOpFactory.INSTANCE;
    public static final MulOpFactory MUL = MulOpFactory.INSTANCE;
    public static final DivOpFactory DIV = DivOpFactory.INSTANCE;

    // Relations
    public static final EqOpFactory EQ = EqOpFactory.INSTANCE;
    public static final NeOpFactory NE = NeOpFactory.INSTANCE;
    public static final GtOpFactory GT = GtOpFactory.INSTANCE;
    public static final GeOpFactory GE = GeOpFactory.INSTANCE;
    public static final LtOpFactory LT = LtOpFactory.INSTANCE;
    public static final LeOpFactory LE = LeOpFactory.INSTANCE;

    // Logics
    public static final AndOp AND = AndOp.INSTANCE;
    public static final OrOp OR = OrOp.INSTANCE;
    public static final AndFun AND_FUN = AndFun.INSTANCE;
    public static final OrFun OR_FUN = OrFun.INSTANCE;
    public static final NotOpFactory NOT = NotOpFactory.INSTANCE;

    // Specials
    public static final IsNullFunFactory IS_NULL = IsNullFunFactory.INSTANCE;
    public static final IsTrueFunFactory IS_TRUE = IsTrueFunFactory.INSTANCE;
    public static final IsFalseFunFactory IS_FALSE = IsFalseFunFactory.INSTANCE;

    // Mathematics
    public static final AbsFunFactory ABS = AbsFunFactory.INSTANCE;
    public static final ModFunFactory MOD = ModFunFactory.INSTANCE;
    public static final MaxFunFactory MAX = MaxFunFactory.INSTANCE;
    public static final MinFunFactory MIN = MinFunFactory.INSTANCE;
    public static final SinFunFactory SIN = SinFunFactory.INSTANCE;
    public static final CosFunFactory COS = CosFunFactory.INSTANCE;
    public static final TanFunFactory TAN = TanFunFactory.INSTANCE;
    public static final AsinFunFactory ASIN = AsinFunFactory.INSTANCE;
    public static final AcosFunFactory ACOS = AcosFunFactory.INSTANCE;
    public static final AtanFunFactory ATAN = AtanFunFactory.INSTANCE;
    public static final SinhFunFactory SINH = SinhFunFactory.INSTANCE;
    public static final CoshFunFactory COSH = CoshFunFactory.INSTANCE;
    public static final TanhFunFactory TANH = TanhFunFactory.INSTANCE;
    public static final ExpFunFactory EXP = ExpFunFactory.INSTANCE;
    public static final LogFunFactory LOG = LogFunFactory.INSTANCE;
    public static final CeilFunFactory CEIL = CeilFunFactory.INSTANCE;
    public static final FloorFunFactory FLOOR = FloorFunFactory.INSTANCE;

    // Strings
    public static final CharLengthFunFactory CHAR_LENGTH = CharLengthFunFactory.INSTANCE;
    public static final ConcatFunFactory CONCAT = ConcatFunFactory.INSTANCE;
    public static final LowerFunFactory LOWER = LowerFunFactory.INSTANCE;
    public static final UpperFunFactory UPPER = UpperFunFactory.INSTANCE;
    public static final LeftFunFactory LEFT = LeftFunFactory.INSTANCE;
    public static final RightFunFactory RIGHT = RightFunFactory.INSTANCE;
    public static final TrimFunFactory TRIM = TrimFunFactory.INSTANCE;
    public static final LTrimFunFactory LTRIM = LTrimFunFactory.INSTANCE;
    public static final RTrimFunFactory RTRIM = RTrimFunFactory.INSTANCE;
    public static final Substr2FunFactory SUBSTR2 = Substr2FunFactory.INSTANCE;
    public static final Substr3FunFactory SUBSTR3 = Substr3FunFactory.INSTANCE;
    public static final Mid2FunFactory MID2 = Mid2FunFactory.INSTANCE;
    public static final Mid3FunFactory MID3 = Mid3FunFactory.INSTANCE;
    public static final RepeatFunFactory REPEAT = RepeatFunFactory.INSTANCE;
    public static final ReverseFunFactory REVERSE = ReverseFunFactory.INSTANCE;
    public static final ReplaceFunFactory REPLACE = ReplaceFunFactory.INSTANCE;
    public static final Locate2FunFactory LOCATE2 = Locate2FunFactory.INSTANCE;
    public static final Locate3FunFactory LOCATE3 = Locate3FunFactory.INSTANCE;
    public static final ConvertTimeFormatFunFactory _CTF = ConvertTimeFormatFunFactory.INSTANCE;
    public static final HexFunFactory HEX = HexFunFactory.INSTANCE;

    // Index
    public static final IndexOp INDEX = IndexOp.INSTANCE;

    // Date and time
    public static final CurrentDateFun CURRENT_DATE = CurrentDateFun.INSTANCE;
    public static final CurrentTimeFun CURRENT_TIME = CurrentTimeFun.INSTANCE;
    public static final CurrentTimestampFun CURRENT_TIMESTAMP = CurrentTimestampFun.INSTANCE;
    public static final DateFormat1FunFactory DATE_FORMAT1 = DateFormat1FunFactory.INSTANCE;
    public static final DateFormat2FunFactory DATE_FORMAT2 = DateFormat2FunFactory.INSTANCE;
    public static final TimeFormat1FunFactory TIME_FORMAT1 = TimeFormat1FunFactory.INSTANCE;
    public static final TimeFormat2FunFactory TIME_FORMAT2 = TimeFormat2FunFactory.INSTANCE;
    public static final TimestampFormat1FunFactory TIMESTAMP_FORMAT1 = TimestampFormat1FunFactory.INSTANCE;
    public static final TimestampFormat2FunFactory TIMESTAMP_FORMAT2 = TimestampFormat2FunFactory.INSTANCE;

    // Collections
    public static final ArrayConstructorOpFactory ARRAY = ArrayConstructorOpFactory.INSTANCE;
    public static final ListConstructorOpFactory LIST = ListConstructorOpFactory.INSTANCE;

    private Exprs() {
    }

    public static @NonNull Val val(Object value) {
        return val(value, Types.valueType(value));
    }

    public static @NonNull Val val(Object value, Type type) {
        if (value != null) {
            Type valueType = Types.valueType(value);
            if (valueType.equals(type) && value instanceof Boolean) {
                return (Boolean) value ? Val.TRUE : Val.FALSE;
            }
            return new Val(value, type);
        }
        return NullCreator.INSTANCE.visit(type);
    }

    public static @NonNull Var var(Object id) {
        return new Var(id, null);
    }

    public static @NonNull Var var(Object id, Type type) {
        return new Var(id, type);
    }

    public static @NonNull NullaryOpExpr op(
        @NonNull NullaryOp op
    ) {
        return new NullaryOpExpr(op);
    }

    public static @NonNull UnaryOpExpr op(
        @NonNull UnaryOp op,
        @NonNull Object operand
    ) {
        Expr expr = transOperand(operand);
        return new UnaryOpExpr(op, expr);
    }

    public static @NonNull BinaryOpExpr op(
        @NonNull BinaryOp op,
        @NonNull Object operand0,
        @NonNull Object operand1
    ) {
        Expr expr0 = transOperand(operand0);
        Expr expr1 = transOperand(operand1);
        if (op.getOpType() == OpType.INDEX) {
            return new IndexOpExpr((IndexOp) op, expr0, expr1);
        } else {
            return new BinaryOpExpr(op, expr0, expr1);
        }
    }

    public static @NonNull TertiaryOpExpr op(
        @NonNull TertiaryOp op,
        @NonNull Object operand0,
        @NonNull Object operand1,
        @NonNull Object operand2
    ) {
        Expr expr0 = transOperand(operand0);
        Expr expr1 = transOperand(operand1);
        Expr expr2 = transOperand(operand2);
        return new TertiaryOpExpr(op, expr0, expr1, expr2);
    }

    public static @NonNull VariadicOpExpr op(
        @NonNull VariadicOp op,
        Object @NonNull ... operands
    ) {
        Expr[] exprs = transOperands(operands);
        return new VariadicOpExpr(op, exprs);
    }

    private static @NonNull Expr transOperand(Object operand) {
        return operand instanceof Expr ? (Expr) operand : val(operand);
    }

    private static Expr @NonNull [] transOperands(Object @NonNull [] operands) {
        return Arrays.stream(operands)
            .map(Exprs::transOperand)
            .toArray(Expr[]::new);
    }

    private static class NullCreator extends TypeVisitorBase<Val, Void> {
        private static final NullCreator INSTANCE = new NullCreator();

        @Override
        public Val visitNullType(@NonNull NullType type, Void obj) {
            return Val.NULL;
        }

        @Override
        public Val visitIntType(@NonNull IntType type, Void obj) {
            return Val.NULL_INT;
        }

        @Override
        public Val visitLongType(@NonNull LongType type, Void obj) {
            return Val.NULL_LONG;
        }

        @Override
        public Val visitFloatType(@NonNull FloatType type, Void obj) {
            return Val.NULL_FLOAT;
        }

        @Override
        public Val visitDoubleType(@NonNull DoubleType type, Void obj) {
            return Val.NULL_DOUBLE;
        }

        @Override
        public Val visitBoolType(@NonNull BoolType type, Void obj) {
            return Val.NULL_BOOL;
        }

        @Override
        public Val visitDecimalType(@NonNull DecimalType type, Void obj) {
            return Val.NULL_DECIMAL;
        }

        @Override
        public Val visitStringType(@NonNull StringType type, Void obj) {
            return Val.NULL_STRING;
        }

        @Override
        public Val visitBytesType(@NonNull BytesType type, Void obj) {
            return Val.NULL_BYTES;
        }

        @Override
        public Val visitDateType(@NonNull DateType type, Void obj) {
            return Val.NULL_DATE;
        }

        @Override
        public Val visitTimeType(@NonNull TimeType type, Void obj) {
            return Val.NULL_TIME;
        }

        @Override
        public Val visitTimestampType(@NonNull TimestampType type, Void obj) {
            return Val.NULL_TIMESTAMP;
        }

        @Override
        public Val visitAnyType(@NonNull AnyType type, Void obj) {
            return Val.NULL_ANY;
        }

        @Override
        public @NonNull Val visitArrayType(@NonNull ArrayType type, Void obj) {
            return new Val(null, type);
        }

        @Override
        public @NonNull Val visitListType(@NonNull ListType type, Void obj) {
            return new Val(null, type);
        }

        @Override
        public @NonNull Val visitMapType(@NonNull MapType type, Void obj) {
            return new Val(null, type);
        }
    }
}
