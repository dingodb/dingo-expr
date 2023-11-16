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
import io.dingodb.expr.runtime.type.TypeVisitorBase;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ArrayBuilder extends TypeVisitorBase<Object, @NonNull Integer> {
    public static final ArrayBuilder INSTANCE = new ArrayBuilder();

    @Override
    public @NonNull Object visitIntType(@NonNull IntType type, @NonNull Integer obj) {
        return new int[obj];
    }

    @Override
    public @NonNull Object visitLongType(@NonNull LongType type, @NonNull Integer obj) {
        return new long[obj];
    }

    @Override
    public @NonNull Object visitFloatType(@NonNull FloatType type, @NonNull Integer obj) {
        return new float[obj];
    }

    @Override
    public @NonNull Object visitDoubleType(@NonNull DoubleType type, @NonNull Integer obj) {
        return new double[obj];
    }

    @Override
    public @NonNull Object visitBoolType(@NonNull BoolType type, @NonNull Integer obj) {
        return new boolean[obj];
    }

    @Override
    public @NonNull Object visitDecimalType(@NonNull DecimalType type, @NonNull Integer obj) {
        return new BigDecimal[obj];
    }

    @Override
    public @NonNull Object visitStringType(@NonNull StringType type, @NonNull Integer obj) {
        return new String[obj];
    }

    @Override
    public @NonNull Object visitBytesType(@NonNull BytesType type, @NonNull Integer obj) {
        return new byte[obj][];
    }

    @Override
    public @NonNull Object visitDateType(@NonNull DateType type, @NonNull Integer obj) {
        return new Date[obj];
    }

    @Override
    public @NonNull Object visitTimeType(@NonNull TimeType type, @NonNull Integer obj) {
        return new Time[obj];
    }

    @Override
    public @NonNull Object visitTimestampType(@NonNull TimestampType type, @NonNull Integer obj) {
        return new Timestamp[obj];
    }

    @Override
    public @NonNull Object visitAnyType(@NonNull AnyType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public @NonNull Object visitArrayType(@NonNull ArrayType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public @NonNull Object visitListType(@NonNull ListType type, @NonNull Integer obj) {
        return new List<?>[obj];
    }

    @Override
    public @NonNull Object visitMapType(@NonNull MapType type, @NonNull Integer obj) {
        return new Map<?, ?>[obj];
    }

    @Override
    public @NonNull Object visitTupleType(@NonNull TupleType type, @NonNull Integer obj) {
        return new Object[obj][type.getSize()];
    }
}
