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

package io.dingodb.expr.runtime.op.index;

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.type.AnyType;
import io.dingodb.expr.runtime.type.ArrayType;
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
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Array;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexArrayOp extends IndexOpFactory {
    private static final long serialVersionUID = 368593503014605079L;

    private final ArrayType originalType;

    public static IndexArrayOp of(@NonNull ArrayType type) {
        Type elementType = type.getElementType();
        if (elementType.isScalar()) {
            return IndexArrayOpCreator.INSTANCE.visit(elementType);
        }
        return new IndexArrayOp(type);
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        return Array.get(value0, (Integer) value1);
    }

    @Override
    public Type getType() {
        return originalType.getElementType();
    }

    @Override
    public Object getKey() {
        return originalType;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class IndexArrayOpCreator extends TypeVisitorBase<IndexArrayOp, Type> {
        private static final IndexArrayOpCreator INSTANCE = new IndexArrayOpCreator();

        private static final IndexArrayOp INDEX_ARRAY_INT = new IndexArrayOp(Types.ARRAY_INT);
        private static final IndexArrayOp INDEX_ARRAY_LONG = new IndexArrayOp(Types.ARRAY_LONG);
        private static final IndexArrayOp INDEX_ARRAY_FLOAT = new IndexArrayOp(Types.ARRAY_FLOAT);
        private static final IndexArrayOp INDEX_ARRAY_DOUBLE = new IndexArrayOp(Types.ARRAY_DOUBLE);
        private static final IndexArrayOp INDEX_ARRAY_BOOL = new IndexArrayOp(Types.ARRAY_BOOL);
        private static final IndexArrayOp INDEX_ARRAY_DECIMAL = new IndexArrayOp(Types.ARRAY_DECIMAL);
        private static final IndexArrayOp INDEX_ARRAY_STRING = new IndexArrayOp(Types.ARRAY_STRING);
        private static final IndexArrayOp INDEX_ARRAY_BYTES = new IndexArrayOp(Types.ARRAY_BYTES);
        private static final IndexArrayOp INDEX_ARRAY_DATE = new IndexArrayOp(Types.ARRAY_DATE);
        private static final IndexArrayOp INDEX_ARRAY_TIME = new IndexArrayOp(Types.ARRAY_TIME);
        private static final IndexArrayOp INDEX_ARRAY_TIMESTAMP = new IndexArrayOp(Types.ARRAY_TIMESTAMP);
        private static final IndexArrayOp INDEX_ARRAY_ANY = new IndexArrayOp(Types.ARRAY_ANY);

        @Override
        public IndexArrayOp visitIntType(@NonNull IntType type, Type obj) {
            return INDEX_ARRAY_INT;
        }

        @Override
        public IndexArrayOp visitLongType(@NonNull LongType type, Type obj) {
            return INDEX_ARRAY_LONG;
        }

        @Override
        public IndexArrayOp visitFloatType(@NonNull FloatType type, Type obj) {
            return INDEX_ARRAY_FLOAT;
        }

        @Override
        public IndexArrayOp visitDoubleType(@NonNull DoubleType type, Type obj) {
            return INDEX_ARRAY_DOUBLE;
        }

        @Override
        public IndexArrayOp visitBoolType(@NonNull BoolType type, Type obj) {
            return INDEX_ARRAY_BOOL;
        }

        @Override
        public IndexArrayOp visitDecimalType(@NonNull DecimalType type, Type obj) {
            return INDEX_ARRAY_DECIMAL;
        }

        @Override
        public IndexArrayOp visitStringType(@NonNull StringType type, Type obj) {
            return INDEX_ARRAY_STRING;
        }

        @Override
        public IndexArrayOp visitBytesType(@NonNull BytesType type, Type obj) {
            return INDEX_ARRAY_BYTES;
        }

        @Override
        public IndexArrayOp visitDateType(@NonNull DateType type, Type obj) {
            return INDEX_ARRAY_DATE;
        }

        @Override
        public IndexArrayOp visitTimeType(@NonNull TimeType type, Type obj) {
            return INDEX_ARRAY_TIME;
        }

        @Override
        public IndexArrayOp visitTimestampType(@NonNull TimestampType type, Type obj) {
            return INDEX_ARRAY_TIMESTAMP;
        }

        @Override
        public IndexArrayOp visitAnyType(@NonNull AnyType type, Type obj) {
            return INDEX_ARRAY_ANY;
        }
    }
}
