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

import io.dingodb.expr.common.type.ArrayType;
import io.dingodb.expr.common.type.ListType;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.utils.ExceptionUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;

abstract class SliceArrayOp extends SliceOp {
    private static final long serialVersionUID = 6362732903056529224L;

    protected SliceArrayOp(@NonNull ArrayType originalType, ArrayType type) {
        super(originalType, type);
    }

    public static @Nullable SliceArrayOp of(@NonNull ArrayType type) {
        Type elementType = type.getElementType();
        if (elementType instanceof ArrayType) {
            return new SliceArrayOfArrayOp(type);
        } else if (elementType instanceof ListType) {
            return new SliceArrayOfListOp(type);
        } else if (elementType instanceof TupleType) {
            return new SliceArrayOfTupleOp(type, Types.ARRAY_ANY);
        }
        return null;
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        int index = (Integer) value1;
        int size = Array.getLength(value0);
        Object result = ArrayBuilder.INSTANCE.visit(type.getElementType(), size);
        for (int i = 0; i < size; ++i) {
            Array.set(result, i, ExceptionUtils.nonNullElement(getValueOf(Array.get(value0, i), index)));
        }
        return result;
    }
}
