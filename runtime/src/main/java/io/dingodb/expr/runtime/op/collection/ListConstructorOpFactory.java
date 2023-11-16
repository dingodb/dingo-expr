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
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.TypeVisitorBase;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ListConstructorOpFactory extends CollectionConstructorOpFactory {
    public static final ListConstructorOpFactory INSTANCE = new ListConstructorOpFactory();

    public static final String NAME = "LIST";

    private static final long serialVersionUID = -3474760405729169933L;

    protected ListConstructorOpFactory() {
    }

    @Override
    public VariadicOp getOp(Object key) {
        return key != null ? ListConstructorOpCreator.INSTANCE.visit((Type) key) : null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class ListConstructorOpCreator extends TypeVisitorBase<ListConstructorOp, Type> {
        private static final ListConstructorOpCreator INSTANCE = new ListConstructorOpCreator();

        private static final ListConstructorOp LIST_INT_CONSTRUCTOR = new ListConstructorOp(Types.INT);
        private static final ListConstructorOp LIST_LONG_CONSTRUCTOR = new ListConstructorOp(Types.LONG);
        private static final ListConstructorOp LIST_FLOAT_CONSTRUCTOR = new ListConstructorOp(Types.FLOAT);
        private static final ListConstructorOp LIST_DOUBLE_CONSTRUCTOR = new ListConstructorOp(Types.DOUBLE);
        private static final ListConstructorOp LIST_BOOL_CONSTRUCTOR = new ListConstructorOp(Types.BOOL);
        private static final ListConstructorOp LIST_DECIMAL_CONSTRUCTOR = new ListConstructorOp(Types.DECIMAL);
        private static final ListConstructorOp LIST_STRING_CONSTRUCTOR = new ListConstructorOp(Types.STRING);
        private static final ListConstructorOp LIST_BYTES_CONSTRUCTOR = new ListConstructorOp(Types.BYTES);
        private static final ListConstructorOp LIST_DATE_CONSTRUCTOR = new ListConstructorOp(Types.DATE);
        private static final ListConstructorOp LIST_TIME_CONSTRUCTOR = new ListConstructorOp(Types.TIME);
        private static final ListConstructorOp LIST_TIMESTAMP_CONSTRUCTOR = new ListConstructorOp(Types.TIMESTAMP);
        private static final ListConstructorOp LIST_ANY_CONSTRUCTOR = new ListConstructorOp(Types.ANY);

        @Override
        public ListConstructorOp visitIntType(@NonNull IntType type, Type obj) {
            return LIST_INT_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitLongType(@NonNull LongType type, Type obj) {
            return LIST_LONG_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitFloatType(@NonNull FloatType type, Type obj) {
            return LIST_FLOAT_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitDoubleType(@NonNull DoubleType type, Type obj) {
            return LIST_DOUBLE_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitBoolType(@NonNull BoolType type, Type obj) {
            return LIST_BOOL_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitDecimalType(@NonNull DecimalType type, Type obj) {
            return LIST_DECIMAL_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitStringType(@NonNull StringType type, Type obj) {
            return LIST_STRING_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitBytesType(@NonNull BytesType type, Type obj) {
            return LIST_BYTES_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitDateType(@NonNull DateType type, Type obj) {
            return LIST_DATE_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitTimeType(@NonNull TimeType type, Type obj) {
            return LIST_TIME_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitTimestampType(@NonNull TimestampType type, Type obj) {
            return LIST_TIMESTAMP_CONSTRUCTOR;
        }

        @Override
        public ListConstructorOp visitAnyType(@NonNull AnyType type, Type obj) {
            return LIST_ANY_CONSTRUCTOR;
        }

        @Override
        public @NonNull ListConstructorOp visitArrayType(@NonNull ArrayType type, Type obj) {
            return new ListConstructorOp(type);
        }

        @Override
        public @NonNull ListConstructorOp visitListType(@NonNull ListType type, Type obj) {
            return new ListConstructorOp(type);
        }

        @Override
        public @NonNull ListConstructorOp visitMapType(@NonNull MapType type, Type obj) {
            return new ListConstructorOp(type);
        }
    }
}
