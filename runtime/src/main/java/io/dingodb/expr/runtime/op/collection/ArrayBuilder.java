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

import io.dingodb.expr.common.type.AnyType;
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
import io.dingodb.expr.common.type.IntervalQuarterType;
import io.dingodb.expr.common.type.IntervalSecondType;
import io.dingodb.expr.common.type.IntervalWeekType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.common.type.ListType;
import io.dingodb.expr.common.type.LongType;
import io.dingodb.expr.common.type.MapType;
import io.dingodb.expr.common.type.StringType;
import io.dingodb.expr.common.type.TimeType;
import io.dingodb.expr.common.type.TimestampType;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.common.type.TypeVisitorBase;
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

    @Override
    public Object visitIntervalYearType(@NonNull IntervalYearType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public Object visitIntervalMonthType(@NonNull IntervalMonthType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public Object visitIntervalQuarterType(@NonNull IntervalQuarterType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public Object visitIntervalDayType(@NonNull IntervalDayType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public Object visitIntervalWeekType(@NonNull IntervalWeekType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public Object visitIntervalHourType(@NonNull IntervalHourType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public Object visitIntervalMinuteType(@NonNull IntervalMinuteType type, @NonNull Integer obj) {
        return new Object[obj];
    }

    @Override
    public Object visitIntervalSecondType(@NonNull IntervalSecondType type, @NonNull Integer obj) {
        return new Object[obj];
    }
}
