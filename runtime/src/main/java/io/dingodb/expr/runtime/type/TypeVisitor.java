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

package io.dingodb.expr.runtime.type;

import org.checkerframework.checker.nullness.qual.NonNull;

public interface TypeVisitor<R, T> {
    R visitNullType(@NonNull NullType type, T obj);

    R visitIntType(@NonNull IntType type, T obj);

    R visitLongType(@NonNull LongType type, T obj);

    R visitFloatType(@NonNull FloatType type, T obj);

    R visitDoubleType(@NonNull DoubleType type, T obj);

    R visitBoolType(@NonNull BoolType type, T obj);

    R visitDecimalType(@NonNull DecimalType type, T obj);

    R visitStringType(@NonNull StringType type, T obj);

    R visitBytesType(@NonNull BytesType type, T obj);

    R visitDateType(@NonNull DateType type, T obj);

    R visitTimeType(@NonNull TimeType type, T obj);

    R visitTimestampType(@NonNull TimestampType type, T obj);

    R visitAnyType(@NonNull AnyType type, T obj);

    R visitArrayType(@NonNull ArrayType type, T obj);

    R visitListType(@NonNull ListType type, T obj);

    R visitMapType(@NonNull MapType type, T obj);

    R visitTupleType(@NonNull TupleType type, T obj);
}
