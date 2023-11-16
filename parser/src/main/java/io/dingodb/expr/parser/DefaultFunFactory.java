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

package io.dingodb.expr.parser;

import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.NullaryOp;
import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.VariadicOp;
import io.dingodb.expr.runtime.op.collection.ArrayConstructorOpFactory;
import io.dingodb.expr.runtime.op.collection.ListConstructorOpFactory;
import io.dingodb.expr.runtime.op.logical.AndFun;
import io.dingodb.expr.runtime.op.logical.OrFun;
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
import io.dingodb.expr.runtime.op.special.IsFalseFunFactory;
import io.dingodb.expr.runtime.op.special.IsNullFunFactory;
import io.dingodb.expr.runtime.op.special.IsTrueFunFactory;
import io.dingodb.expr.runtime.op.string.CharLengthFunFactory;
import io.dingodb.expr.runtime.op.string.ConcatFunFactory;
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
import io.dingodb.expr.runtime.type.BoolType;
import io.dingodb.expr.runtime.type.BytesType;
import io.dingodb.expr.runtime.type.DateType;
import io.dingodb.expr.runtime.type.DecimalType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.expr.runtime.type.LongType;
import io.dingodb.expr.runtime.type.StringType;
import io.dingodb.expr.runtime.type.TimeType;
import io.dingodb.expr.runtime.type.TimestampType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.TreeMap;

public class DefaultFunFactory implements FunFactory {
    protected final Map<String, NullaryOp> nullaryFunMap;
    protected final Map<String, UnaryOp> unaryFunMap;
    protected final Map<String, BinaryOp> binaryFunMap;
    protected final Map<String, TertiaryOp> tertiaryFunMap;
    protected final Map<String, VariadicOp> variadicFunMap;

    public DefaultFunFactory() {
        nullaryFunMap = new TreeMap<>(String::compareToIgnoreCase);
        unaryFunMap = new TreeMap<>(String::compareToIgnoreCase);
        binaryFunMap = new TreeMap<>(String::compareToIgnoreCase);
        tertiaryFunMap = new TreeMap<>(String::compareToIgnoreCase);
        variadicFunMap = new TreeMap<>(String::compareToIgnoreCase);

        // Castings
        registerUnaryFun(IntType.NAME, Exprs.TO_INT);
        registerUnaryFun(LongType.NAME, Exprs.TO_LONG);
        registerUnaryFun(FloatType.NAME, Exprs.TO_FLOAT);
        registerUnaryFun(DoubleType.NAME, Exprs.TO_DOUBLE);
        registerUnaryFun(BoolType.NAME, Exprs.TO_BOOL);
        registerUnaryFun(DecimalType.NAME, Exprs.TO_DECIMAL);
        registerUnaryFun(StringType.NAME, Exprs.TO_STRING);
        registerUnaryFun(BytesType.NAME, Exprs.TO_BYTES);
        registerUnaryFun(DateType.NAME, Exprs.TO_DATE);
        registerUnaryFun(TimeType.NAME, Exprs.TO_TIME);
        registerUnaryFun(TimestampType.NAME, Exprs.TO_TIMESTAMP);

        // Mathematical
        registerUnaryFun(AbsFunFactory.NAME, Exprs.ABS);
        registerBinaryFun(ModFunFactory.NAME, Exprs.MOD);
        registerBinaryFun(MinFunFactory.NAME, Exprs.MIN);
        registerBinaryFun(MaxFunFactory.NAME, Exprs.MAX);
        registerUnaryFun(SinFunFactory.NAME, Exprs.SIN);
        registerUnaryFun(CosFunFactory.NAME, Exprs.COS);
        registerUnaryFun(TanFunFactory.NAME, Exprs.TAN);
        registerUnaryFun(AsinFunFactory.NAME, Exprs.ASIN);
        registerUnaryFun(AcosFunFactory.NAME, Exprs.ACOS);
        registerUnaryFun(AtanFunFactory.NAME, Exprs.ATAN);
        registerUnaryFun(SinhFunFactory.NAME, Exprs.SINH);
        registerUnaryFun(CoshFunFactory.NAME, Exprs.COSH);
        registerUnaryFun(TanhFunFactory.NAME, Exprs.TANH);
        registerUnaryFun(ExpFunFactory.NAME, Exprs.EXP);
        registerUnaryFun(LogFunFactory.NAME, Exprs.LOG);
        registerUnaryFun(CeilFunFactory.NAME, Exprs.CEIL);
        registerUnaryFun(FloorFunFactory.NAME, Exprs.FLOOR);

        // Logical and special
        registerVariadicFun(AndFun.NAME, Exprs.AND_FUN);
        registerVariadicFun(OrFun.NAME, Exprs.OR_FUN);
        registerUnaryFun(IsNullFunFactory.NAME, Exprs.IS_NULL);
        registerUnaryFun(IsTrueFunFactory.NAME, Exprs.IS_TRUE);
        registerUnaryFun(IsFalseFunFactory.NAME, Exprs.IS_FALSE);

        // String functions
        registerUnaryFun(CharLengthFunFactory.NAME, Exprs.CHAR_LENGTH);
        registerBinaryFun(ConcatFunFactory.NAME, Exprs.CONCAT);
        registerUnaryFun(LowerFunFactory.NAME, Exprs.LOWER);
        registerUnaryFun(UpperFunFactory.NAME, Exprs.UPPER);
        registerBinaryFun(LeftFunFactory.NAME, Exprs.LEFT);
        registerBinaryFun(RightFunFactory.NAME, Exprs.RIGHT);
        registerUnaryFun(TrimFunFactory.NAME, Exprs.TRIM);
        registerUnaryFun(LTrimFunFactory.NAME, Exprs.LTRIM);
        registerUnaryFun(RTrimFunFactory.NAME, Exprs.RTRIM);
        registerBinaryFun(Substr2FunFactory.NAME, Exprs.SUBSTR2);
        registerTertiaryFun(Substr3FunFactory.NAME, Exprs.SUBSTR3);
        registerBinaryFun(Mid2FunFactory.NAME, Exprs.MID2);
        registerTertiaryFun(Mid3FunFactory.NAME, Exprs.MID3);
        registerBinaryFun(RepeatFunFactory.NAME, Exprs.REPEAT);
        registerUnaryFun(ReverseFunFactory.NAME, Exprs.REVERSE);
        registerTertiaryFun(ReplaceFunFactory.NAME, Exprs.REPLACE);
        registerBinaryFun(Locate2FunFactory.NAME, Exprs.LOCATE2);
        registerTertiaryFun(Locate3FunFactory.NAME, Exprs.LOCATE3);
        registerUnaryFun(HexFunFactory.NAME, Exprs.HEX);

        // Time functions
        registerNullaryFun(CurrentDateFun.NAME, Exprs.CURRENT_DATE);
        registerNullaryFun(CurrentTimeFun.NAME, Exprs.CURRENT_TIME);
        registerNullaryFun(CurrentTimestampFun.NAME, Exprs.CURRENT_TIMESTAMP);
        registerUnaryFun(DateFormat1FunFactory.NAME, Exprs.DATE_FORMAT1);
        registerBinaryFun(DateFormat2FunFactory.NAME, Exprs.DATE_FORMAT2);
        registerUnaryFun(TimeFormat1FunFactory.NAME, Exprs.TIME_FORMAT1);
        registerBinaryFun(TimeFormat2FunFactory.NAME, Exprs.TIME_FORMAT2);
        registerUnaryFun(TimestampFormat1FunFactory.NAME, Exprs.TIMESTAMP_FORMAT1);
        registerBinaryFun(TimestampFormat2FunFactory.NAME, Exprs.TIMESTAMP_FORMAT2);

        // Collection functions
        registerVariadicFun(ArrayConstructorOpFactory.NAME, Exprs.ARRAY);
        registerVariadicFun(ListConstructorOpFactory.NAME, Exprs.LIST);
    }

    @Override
    public NullaryOp getNullaryFun(@NonNull String funName) {
        return nullaryFunMap.get(funName);
    }

    @Override
    public void registerNullaryFun(@NonNull String funName, @NonNull NullaryOp op) {
        nullaryFunMap.put(funName, op);
    }

    @Override
    public UnaryOp getUnaryFun(@NonNull String funName) {
        return unaryFunMap.get(funName);
    }

    @Override
    public void registerUnaryFun(@NonNull String funName, @NonNull UnaryOp op) {
        unaryFunMap.put(funName, op);
    }

    @Override
    public BinaryOp getBinaryFun(@NonNull String funName) {
        return binaryFunMap.get(funName);
    }

    @Override
    public void registerBinaryFun(@NonNull String funName, @NonNull BinaryOp op) {
        binaryFunMap.put(funName, op);
    }

    @Override
    public TertiaryOp getTertiaryFun(@NonNull String funName) {
        return tertiaryFunMap.get(funName);
    }

    @Override
    public void registerTertiaryFun(@NonNull String funName, @NonNull TertiaryOp op) {
        tertiaryFunMap.put(funName, op);
    }

    @Override
    public VariadicOp getVariadicFun(@NonNull String funName) {
        return variadicFunMap.get(funName);
    }

    @Override
    public void registerVariadicFun(@NonNull String funName, @NonNull VariadicOp op) {
        variadicFunMap.put(funName, op);
    }
}
