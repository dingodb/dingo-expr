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

package io.dingodb.expr.runtime.compiler;

import io.dingodb.expr.common.type.ArrayType;
import io.dingodb.expr.common.type.BoolType;
import io.dingodb.expr.common.type.BytesType;
import io.dingodb.expr.common.type.DateType;
import io.dingodb.expr.common.type.DecimalType;
import io.dingodb.expr.common.type.DoubleType;
import io.dingodb.expr.common.type.FloatType;
import io.dingodb.expr.common.type.IntType;
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalHourType;
import io.dingodb.expr.common.type.IntervalMinuteType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalSecondType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.common.type.ListType;
import io.dingodb.expr.common.type.LongType;
import io.dingodb.expr.common.type.StringType;
import io.dingodb.expr.common.type.TimeType;
import io.dingodb.expr.common.type.TimestampType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.TypeVisitorBase;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.exception.ExprCompileException;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.collection.CastArrayOpFactory;
import io.dingodb.expr.runtime.op.collection.CastListOpFactory;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class CastingFactory {
    private CastingFactory() {
    }

    public static @NonNull UnaryOp get(Type toType, @NonNull ExprConfig config) {
        UnaryOp op = CastOpSelector.INSTANCE.visit(toType, config);
        if (op != null) {
            return op;
        }
        throw new ExprCompileException("Cannot cast to " + toType + ".");
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class CastOpSelector extends TypeVisitorBase<UnaryOp, ExprConfig> {
        private static final CastOpSelector INSTANCE = new CastOpSelector();

        @Override
        public UnaryOp visitIntType(@NonNull IntType type, @NonNull ExprConfig obj) {
            return obj.withRangeCheck() ? Exprs.TO_INT_C : Exprs.TO_INT;
        }

        @Override
        public UnaryOp visitLongType(@NonNull LongType type, @NonNull ExprConfig obj) {
            return obj.withRangeCheck() ? Exprs.TO_LONG_C : Exprs.TO_LONG;
        }

        @Override
        public UnaryOp visitFloatType(@NonNull FloatType type, ExprConfig obj) {
            return Exprs.TO_FLOAT;
        }

        @Override
        public UnaryOp visitDoubleType(@NonNull DoubleType type, ExprConfig obj) {
            return Exprs.TO_DOUBLE;
        }

        @Override
        public UnaryOp visitBoolType(@NonNull BoolType type, ExprConfig obj) {
            return Exprs.TO_BOOL;
        }

        @Override
        public UnaryOp visitDecimalType(@NonNull DecimalType type, ExprConfig obj) {
            return Exprs.TO_DECIMAL;
        }

        @Override
        public UnaryOp visitStringType(@NonNull StringType type, ExprConfig obj) {
            return Exprs.TO_STRING;
        }

        @Override
        public UnaryOp visitBytesType(@NonNull BytesType type, ExprConfig obj) {
            return Exprs.TO_BYTES;
        }

        @Override
        public UnaryOp visitDateType(@NonNull DateType type, ExprConfig obj) {
            return Exprs.TO_DATE;
        }

        @Override
        public UnaryOp visitTimeType(@NonNull TimeType type, ExprConfig obj) {
            return Exprs.TO_TIME;
        }

        @Override
        public UnaryOp visitTimestampType(@NonNull TimestampType type, ExprConfig obj) {
            return Exprs.TO_TIMESTAMP;
        }

        @Override
        public UnaryOp visitArrayType(@NonNull ArrayType type, ExprConfig obj) {
            return CastArrayOpSelector.INSTANCE.visit(type.getElementType(), obj);
        }

        @Override
        public UnaryOp visitListType(@NonNull ListType type, ExprConfig obj) {
            return CastListOpSelector.INSTANCE.visit(type.getElementType(), obj);
        }

        @Override
        public UnaryOp visitIntervalYearType(@NonNull IntervalYearType type, ExprConfig obj) {
            return Exprs.TO_STRING;
        }

        @Override
        public UnaryOp visitIntervalMonthType(@NonNull IntervalMonthType type, ExprConfig obj) {
            return Exprs.TO_STRING;
        }

        @Override
        public UnaryOp visitIntervalDayType(@NonNull IntervalDayType type, ExprConfig obj) {
            return Exprs.TO_STRING;
        }

        @Override
        public UnaryOp visitIntervalHourType(@NonNull IntervalHourType type, ExprConfig obj) {
            return Exprs.TO_STRING;
        }

        @Override
        public UnaryOp visitIntervalMinuteType(@NonNull IntervalMinuteType type, ExprConfig obj) {
            return Exprs.TO_STRING;
        }

        @Override
        public UnaryOp visitIntervalSecondType(@NonNull IntervalSecondType type, ExprConfig obj) {
            return Exprs.TO_STRING;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class CastArrayOpSelector extends TypeVisitorBase<CastArrayOpFactory, ExprConfig> {
        private static final CastArrayOpSelector INSTANCE = new CastArrayOpSelector();

        @Override
        public CastArrayOpFactory visitIntType(@NonNull IntType type, @NonNull ExprConfig obj) {
            return obj.withRangeCheck() ? Exprs.TO_ARRAY_INT_C : Exprs.TO_ARRAY_INT;
        }

        @Override
        public CastArrayOpFactory visitLongType(@NonNull LongType type, @NonNull ExprConfig obj) {
            return obj.withRangeCheck() ? Exprs.TO_ARRAY_LONG_C : Exprs.TO_ARRAY_LONG;
        }

        @Override
        public CastArrayOpFactory visitFloatType(@NonNull FloatType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_FLOAT;
        }

        @Override
        public CastArrayOpFactory visitDoubleType(@NonNull DoubleType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_DOUBLE;
        }

        @Override
        public CastArrayOpFactory visitBoolType(@NonNull BoolType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_BOOL;
        }

        @Override
        public CastArrayOpFactory visitDecimalType(@NonNull DecimalType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_DECIMAL;
        }

        @Override
        public CastArrayOpFactory visitStringType(@NonNull StringType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_STRING;
        }

        @Override
        public CastArrayOpFactory visitBytesType(@NonNull BytesType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_BYTES;
        }

        @Override
        public CastArrayOpFactory visitDateType(@NonNull DateType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_DATE;
        }

        @Override
        public CastArrayOpFactory visitTimeType(@NonNull TimeType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_TIME;
        }

        @Override
        public CastArrayOpFactory visitTimestampType(@NonNull TimestampType type, ExprConfig obj) {
            return Exprs.TO_ARRAY_TIMESTAMP;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class CastListOpSelector extends TypeVisitorBase<CastListOpFactory, ExprConfig> {
        private static final CastListOpSelector INSTANCE = new CastListOpSelector();

        @Override
        public CastListOpFactory visitIntType(@NonNull IntType type, @NonNull ExprConfig obj) {
            return obj.withRangeCheck() ? Exprs.TO_LIST_INT_C : Exprs.TO_LIST_INT;
        }

        @Override
        public CastListOpFactory visitLongType(@NonNull LongType type, @NonNull ExprConfig obj) {
            return obj.withRangeCheck() ? Exprs.TO_LIST_LONG_C : Exprs.TO_LIST_LONG;
        }

        @Override
        public CastListOpFactory visitFloatType(@NonNull FloatType type, ExprConfig obj) {
            return Exprs.TO_LIST_FLOAT;
        }

        @Override
        public CastListOpFactory visitDoubleType(@NonNull DoubleType type, ExprConfig obj) {
            return Exprs.TO_LIST_DOUBLE;
        }

        @Override
        public CastListOpFactory visitBoolType(@NonNull BoolType type, ExprConfig obj) {
            return Exprs.TO_LIST_BOOL;
        }

        @Override
        public CastListOpFactory visitDecimalType(@NonNull DecimalType type, ExprConfig obj) {
            return Exprs.TO_LIST_DECIMAL;
        }

        @Override
        public CastListOpFactory visitStringType(@NonNull StringType type, ExprConfig obj) {
            return Exprs.TO_LIST_STRING;
        }

        @Override
        public CastListOpFactory visitBytesType(@NonNull BytesType type, ExprConfig obj) {
            return Exprs.TO_LIST_BYTES;
        }

        @Override
        public CastListOpFactory visitDateType(@NonNull DateType type, ExprConfig obj) {
            return Exprs.TO_LIST_DATE;
        }

        @Override
        public CastListOpFactory visitTimeType(@NonNull TimeType type, ExprConfig obj) {
            return Exprs.TO_LIST_TIME;
        }

        @Override
        public CastListOpFactory visitTimestampType(@NonNull TimestampType type, ExprConfig obj) {
            return Exprs.TO_LIST_TIMESTAMP;
        }
    }
}
