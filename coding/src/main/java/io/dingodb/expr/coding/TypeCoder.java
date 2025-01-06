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

import io.dingodb.expr.common.type.BoolType;
import io.dingodb.expr.common.type.DateType;
import io.dingodb.expr.common.type.DecimalType;
import io.dingodb.expr.common.type.DoubleType;
import io.dingodb.expr.common.type.FloatType;
import io.dingodb.expr.common.type.IntType;
import io.dingodb.expr.common.type.LongType;
import io.dingodb.expr.common.type.StringType;
import io.dingodb.expr.common.type.TypeVisitorBase;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class TypeCoder extends TypeVisitorBase<Byte, Void> {
    public static final byte TYPE_INT32 = (byte) 0x01;
    public static final byte TYPE_INT64 = (byte) 0x02;
    public static final byte TYPE_BOOL = (byte) 0x03;
    public static final byte TYPE_FLOAT = (byte) 0x04;
    public static final byte TYPE_DOUBLE = (byte) 0x05;
    public static final byte TYPE_DECIMAL = (byte) 0x06;
    public static final byte TYPE_STRING = (byte) 0x07;
    public static final byte TYPE_DATE = (byte) 0x08;
    static final TypeCoder INSTANCE = new TypeCoder();

    @Override
    public Byte visitIntType(@NonNull IntType type, Void obj) {
        return TYPE_INT32;
    }

    @Override
    public Byte visitLongType(@NonNull LongType type, Void obj) {
        return TYPE_INT64;
    }

    @Override
    public Byte visitFloatType(@NonNull FloatType type, Void obj) {
        return TYPE_FLOAT;
    }

    @Override
    public Byte visitDoubleType(@NonNull DoubleType type, Void obj) {
        return TYPE_DOUBLE;
    }

    @Override
    public Byte visitBoolType(@NonNull BoolType type, Void obj) {
        return TYPE_BOOL;
    }

    @Override
    public Byte visitDecimalType(@NonNull DecimalType type, Void obj) {
        // TODO: Decimal is not supported in libexpr.
        return null;
    }

    @Override
    public Byte visitStringType(@NonNull StringType type, Void obj) {
        return TYPE_STRING;
    }

    @Override
    public Byte visitDateType(@NonNull DateType type, Void obj) {
        return TYPE_DATE;
    }
}
