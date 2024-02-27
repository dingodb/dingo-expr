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
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.AnyType;
import io.dingodb.expr.runtime.type.BoolType;
import io.dingodb.expr.runtime.type.BytesType;
import io.dingodb.expr.runtime.type.DateType;
import io.dingodb.expr.runtime.type.DecimalType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.LongType;
import io.dingodb.expr.runtime.type.StringType;
import io.dingodb.expr.runtime.type.TimeType;
import io.dingodb.expr.runtime.type.TimestampType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.TypeVisitorBase;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexListOp extends IndexOpFactory {
    private static final long serialVersionUID = -7901415430051702471L;

    @Getter
    private final Type type;

    public static IndexListOp of(@NonNull ListType type) {
        Type elementType = type.getElementType();
        if (elementType.isScalar()) {
            return IndexListOpCreator.INSTANCE.visit(elementType);
        }
        return new IndexListOp(type);
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        return ((List<?>) value0).get((int) value1);
    }

    @Override
    public OpKey getKey() {
        return type;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class IndexListOpCreator extends TypeVisitorBase<IndexListOp, Type> {
        private static final IndexListOpCreator INSTANCE = new IndexListOpCreator();

        private static final IndexListOp INDEX_LIST_INT = new IndexListOp(Types.LIST_INT);
        private static final IndexListOp INDEX_LIST_LONG = new IndexListOp(Types.LIST_LONG);
        private static final IndexListOp INDEX_LIST_FLOAT = new IndexListOp(Types.LIST_FLOAT);
        private static final IndexListOp INDEX_LIST_DOUBLE = new IndexListOp(Types.LIST_DOUBLE);
        private static final IndexListOp INDEX_LIST_BOOL = new IndexListOp(Types.LIST_BOOL);
        private static final IndexListOp INDEX_LIST_DECIMAL = new IndexListOp(Types.LIST_DECIMAL);
        private static final IndexListOp INDEX_LIST_STRING = new IndexListOp(Types.LIST_STRING);
        private static final IndexListOp INDEX_LIST_BYTES = new IndexListOp(Types.LIST_BYTES);
        private static final IndexListOp INDEX_LIST_DATE = new IndexListOp(Types.LIST_DATE);
        private static final IndexListOp INDEX_LIST_TIME = new IndexListOp(Types.LIST_TIME);
        private static final IndexListOp INDEX_LIST_TIMESTAMP = new IndexListOp(Types.LIST_TIMESTAMP);
        private static final IndexListOp INDEX_LIST_ANY = new IndexListOp(Types.LIST_ANY);

        @Override
        public IndexListOp visitIntType(@NonNull IntType type, Type obj) {
            return INDEX_LIST_INT;
        }

        @Override
        public IndexListOp visitLongType(@NonNull LongType type, Type obj) {
            return INDEX_LIST_LONG;
        }

        @Override
        public IndexListOp visitFloatType(@NonNull FloatType type, Type obj) {
            return INDEX_LIST_FLOAT;
        }

        @Override
        public IndexListOp visitDoubleType(@NonNull DoubleType type, Type obj) {
            return INDEX_LIST_DOUBLE;
        }

        @Override
        public IndexListOp visitBoolType(@NonNull BoolType type, Type obj) {
            return INDEX_LIST_BOOL;
        }

        @Override
        public IndexListOp visitDecimalType(@NonNull DecimalType type, Type obj) {
            return INDEX_LIST_DECIMAL;
        }

        @Override
        public IndexListOp visitStringType(@NonNull StringType type, Type obj) {
            return INDEX_LIST_STRING;
        }

        @Override
        public IndexListOp visitBytesType(@NonNull BytesType type, Type obj) {
            return INDEX_LIST_BYTES;
        }

        @Override
        public IndexListOp visitDateType(@NonNull DateType type, Type obj) {
            return INDEX_LIST_DATE;
        }

        @Override
        public IndexListOp visitTimeType(@NonNull TimeType type, Type obj) {
            return INDEX_LIST_TIME;
        }

        @Override
        public IndexListOp visitTimestampType(@NonNull TimestampType type, Type obj) {
            return INDEX_LIST_TIMESTAMP;
        }

        @Override
        public IndexListOp visitAnyType(@NonNull AnyType type, Type obj) {
            return INDEX_LIST_ANY;
        }
    }
}
