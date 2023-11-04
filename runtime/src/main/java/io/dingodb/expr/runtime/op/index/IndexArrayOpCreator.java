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
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class IndexArrayOpCreator extends TypeVisitorBase<IndexOp, Type> {
    static final IndexArrayOpCreator INSTANCE = new IndexArrayOpCreator();

    @Override
    public IndexOp visitIntType(@NonNull IntType type, Type obj) {
        return IndexArrayInt.INSTANCE;
    }

    @Override
    public IndexOp visitLongType(@NonNull LongType type, Type obj) {
        return IndexArrayLong.INSTANCE;
    }

    @Override
    public IndexOp visitFloatType(@NonNull FloatType type, Type obj) {
        return IndexArrayFloat.INSTANCE;
    }

    @Override
    public IndexOp visitDoubleType(@NonNull DoubleType type, Type obj) {
        return IndexArrayDouble.INSTANCE;
    }

    @Override
    public IndexOp visitBoolType(@NonNull BoolType type, Type obj) {
        return IndexArrayBool.INSTANCE;
    }

    @Override
    public IndexOp visitDecimalType(@NonNull DecimalType type, Type obj) {
        return IndexArrayDecimal.INSTANCE;
    }

    @Override
    public IndexOp visitStringType(@NonNull StringType type, Type obj) {
        return IndexArrayString.INSTANCE;
    }

    @Override
    public IndexOp visitBytesType(@NonNull BytesType type, Type obj) {
        return new IndexArrayAny(obj);
    }

    @Override
    public IndexOp visitDateType(@NonNull DateType type, Type obj) {
        return IndexArrayDate.INSTANCE;
    }

    @Override
    public IndexOp visitTimeType(@NonNull TimeType type, Type obj) {
        return IndexArrayTime.INSTANCE;
    }

    @Override
    public IndexOp visitTimestampType(@NonNull TimestampType type, Type obj) {
        return IndexArrayTimestamp.INSTANCE;
    }

    @Override
    public IndexOp visitAnyType(@NonNull AnyType type, Type obj) {
        return new IndexArrayAny(obj);
    }

    @Override
    public IndexOp visitArrayType(@NonNull ArrayType type, Type obj) {
        return new IndexArrayAny(obj);
    }

    @Override
    public IndexOp visitListType(@NonNull ListType type, Type obj) {
        return new IndexArrayAny(obj);
    }

    @Override
    public IndexOp visitMapType(@NonNull MapType type, Type obj) {
        return new IndexArrayAny(obj);
    }
}
