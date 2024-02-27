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
import io.dingodb.expr.runtime.type.BoolType;
import io.dingodb.expr.runtime.type.BytesType;
import io.dingodb.expr.runtime.type.DateType;
import io.dingodb.expr.runtime.type.DecimalType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.IntType;
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

public class MapConstructorOpFactory extends CollectionConstructorOpFactory {
    public static final MapConstructorOpFactory INSTANCE = new MapConstructorOpFactory();

    public static final String NAME = "MAP";

    private static final long serialVersionUID = 5608719489744552506L;

    protected MapConstructorOpFactory() {
    }

    @Override
    public VariadicOp getOp(OpKey key) {
        if (key != null) {
            MapType type = (MapType) key;
            if (type.getKeyType().equals(Types.STRING) && type.getValueType().isScalar()) {
                return StringMapConstructorOpCreator.INSTANCE.visit(type.getValueType());
            }
            return new MapConstructorOp(type);
        }
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public OpKey keyOf(@NonNull Type @NonNull ... types) {
        int size = types.length;
        if (size == 0) {
            return Types.MAP_ANY_ANY;
        } else if (size % 2 == 0) {
            Type keyType = types[0];
            Type valueType = types[1];
            for (int i = 2; i < types.length; i += 2) {
                if (!types[i].equals(keyType)) {
                    keyType = Types.ANY;
                    break;
                }
                if (!types[i + 1].equals(valueType)) {
                    valueType = Types.ANY;
                    break;
                }
            }
            return Types.map(keyType, valueType);
        }
        return null;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class StringMapConstructorOpCreator extends TypeVisitorBase<MapConstructorOp, Type> {
        private static final StringMapConstructorOpCreator INSTANCE = new StringMapConstructorOpCreator();

        private static final MapConstructorOp MAP_STRING_INT_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_INT);
        private static final MapConstructorOp MAP_STRING_LONG_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_LONG);
        private static final MapConstructorOp MAP_STRING_FLOAT_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_FLOAT);
        private static final MapConstructorOp MAP_STRING_DOUBLE_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_DOUBLE);
        private static final MapConstructorOp MAP_STRING_BOOL_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_BOOL);
        private static final MapConstructorOp MAP_STRING_DECIMAL_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_DECIMAL);
        private static final MapConstructorOp MAP_STRING_STRING_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_STRING);
        private static final MapConstructorOp MAP_STRING_BYTES_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_BYTES);
        private static final MapConstructorOp MAP_STRING_DATE_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_DATE);
        private static final MapConstructorOp MAP_STRING_TIME_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_TIME);
        private static final MapConstructorOp MAP_STRING_TIMESTAMP_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_TIMESTAMP);
        private static final MapConstructorOp MAP_STRING_ANY_CONSTRUCTOR
            = new MapConstructorOp(Types.MAP_STRING_ANY);

        @Override
        public MapConstructorOp visitIntType(@NonNull IntType type, Type obj) {
            return MAP_STRING_INT_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitLongType(@NonNull LongType type, Type obj) {
            return MAP_STRING_LONG_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitFloatType(@NonNull FloatType type, Type obj) {
            return MAP_STRING_FLOAT_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitDoubleType(@NonNull DoubleType type, Type obj) {
            return MAP_STRING_DOUBLE_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitBoolType(@NonNull BoolType type, Type obj) {
            return MAP_STRING_BOOL_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitDecimalType(@NonNull DecimalType type, Type obj) {
            return MAP_STRING_DECIMAL_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitStringType(@NonNull StringType type, Type obj) {
            return MAP_STRING_STRING_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitBytesType(@NonNull BytesType type, Type obj) {
            return MAP_STRING_BYTES_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitDateType(@NonNull DateType type, Type obj) {
            return MAP_STRING_DATE_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitTimeType(@NonNull TimeType type, Type obj) {
            return MAP_STRING_TIME_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitTimestampType(@NonNull TimestampType type, Type obj) {
            return MAP_STRING_TIMESTAMP_CONSTRUCTOR;
        }

        @Override
        public MapConstructorOp visitAnyType(@NonNull AnyType type, Type obj) {
            return MAP_STRING_ANY_CONSTRUCTOR;
        }
    }
}
