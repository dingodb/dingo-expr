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

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.exception.ExprCompileException;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.op.UnaryOp;
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
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.TypeVisitorBase;
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
            return obj.isDoCastingCheck() ? Exprs.TO_INT_C : Exprs.TO_INT;
        }

        @Override
        public UnaryOp visitLongType(@NonNull LongType type, @NonNull ExprConfig obj) {
            return obj.isDoCastingCheck() ? Exprs.TO_LONG_C : Exprs.TO_LONG;
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
    }
}
