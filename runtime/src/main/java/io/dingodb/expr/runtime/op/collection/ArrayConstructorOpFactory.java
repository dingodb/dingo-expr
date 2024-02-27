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

package io.dingodb.expr.runtime.op.collection;

import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.VariadicOp;
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
import io.dingodb.expr.runtime.type.StringType;
import io.dingodb.expr.runtime.type.TimeType;
import io.dingodb.expr.runtime.type.TimestampType;
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.TypeVisitorBase;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ArrayConstructorOpFactory extends CollectionConstructorOpFactory {
    public static final ArrayConstructorOpFactory INSTANCE = new ArrayConstructorOpFactory();

    public static final String NAME = "ARRAY";

    private static final long serialVersionUID = 2591275222506422507L;

    protected ArrayConstructorOpFactory() {
    }

    @Override
    public VariadicOp getOp(OpKey key) {
        return key != null ? ArrayConstructorOpCreator.INSTANCE.visit((Type) key) : null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class ArrayConstructorOpCreator extends TypeVisitorBase<ArrayConstructorOp, Type> {
        private static final ArrayConstructorOpCreator INSTANCE = new ArrayConstructorOpCreator();

        private static final ArrayConstructorOp ARRAY_INT_CONSTRUCTOR = new ArrayConstructorOp(Types.INT);
        private static final ArrayConstructorOp ARRAY_LONG_CONSTRUCTOR = new ArrayConstructorOp(Types.LONG);
        private static final ArrayConstructorOp ARRAY_FLOAT_CONSTRUCTOR = new ArrayConstructorOp(Types.FLOAT);
        private static final ArrayConstructorOp ARRAY_DOUBLE_CONSTRUCTOR = new ArrayConstructorOp(Types.DOUBLE);
        private static final ArrayConstructorOp ARRAY_BOOL_CONSTRUCTOR = new ArrayConstructorOp(Types.BOOL);
        private static final ArrayConstructorOp ARRAY_DECIMAL_CONSTRUCTOR = new ArrayConstructorOp(Types.DECIMAL);
        private static final ArrayConstructorOp ARRAY_STRING_CONSTRUCTOR = new ArrayConstructorOp(Types.STRING);
        private static final ArrayConstructorOp ARRAY_BYTES_CONSTRUCTOR = new ArrayConstructorOp(Types.BYTES);
        private static final ArrayConstructorOp ARRAY_DATE_CONSTRUCTOR = new ArrayConstructorOp(Types.DATE);
        private static final ArrayConstructorOp ARRAY_TIME_CONSTRUCTOR = new ArrayConstructorOp(Types.TIME);
        private static final ArrayConstructorOp ARRAY_TIMESTAMP_CONSTRUCTOR = new ArrayConstructorOp(Types.TIMESTAMP);
        private static final ArrayConstructorOp ARRAY_ANY_CONSTRUCTOR = new ArrayConstructorOp(Types.ANY);

        @Override
        public ArrayConstructorOp visitIntType(@NonNull IntType type, Type obj) {
            return ARRAY_INT_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitLongType(@NonNull LongType type, Type obj) {
            return ARRAY_LONG_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitFloatType(@NonNull FloatType type, Type obj) {
            return ARRAY_FLOAT_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitDoubleType(@NonNull DoubleType type, Type obj) {
            return ARRAY_DOUBLE_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitBoolType(@NonNull BoolType type, Type obj) {
            return ARRAY_BOOL_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitDecimalType(@NonNull DecimalType type, Type obj) {
            return ARRAY_DECIMAL_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitStringType(@NonNull StringType type, Type obj) {
            return ARRAY_STRING_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitBytesType(@NonNull BytesType type, Type obj) {
            return ARRAY_BYTES_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitDateType(@NonNull DateType type, Type obj) {
            return ARRAY_DATE_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitTimeType(@NonNull TimeType type, Type obj) {
            return ARRAY_TIME_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitTimestampType(@NonNull TimestampType type, Type obj) {
            return ARRAY_TIMESTAMP_CONSTRUCTOR;
        }

        @Override
        public ArrayConstructorOp visitAnyType(@NonNull AnyType type, Type obj) {
            return ARRAY_ANY_CONSTRUCTOR;
        }

        @Override
        public @NonNull ArrayConstructorOp visitArrayType(@NonNull ArrayType type, Type obj) {
            return new ArrayConstructorOp(type);
        }

        @Override
        public @NonNull ArrayConstructorOp visitListType(@NonNull ListType type, Type obj) {
            return new ArrayConstructorOp(type);
        }

        @Override
        public @NonNull ArrayConstructorOp visitMapType(@NonNull MapType type, Type obj) {
            return new ArrayConstructorOp(type);
        }

        @Override
        public @NonNull ArrayConstructorOp visitTupleType(@NonNull TupleType type, Type obj) {
            return new ArrayConstructorOp(type);
        }
    }
}
