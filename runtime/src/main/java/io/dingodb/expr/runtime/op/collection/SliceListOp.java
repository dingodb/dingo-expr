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

import io.dingodb.expr.runtime.type.ArrayType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

class SliceListOp extends SliceOpFactory {
    private static final long serialVersionUID = -1472675231395486262L;

    protected final ListType originalType;

    protected SliceListOp(@NonNull ListType originalType) {
        this.originalType = originalType;
    }

    public static @Nullable SliceListOp of(@NonNull ListType type) {
        Type elementType = type.getElementType();
        if (elementType instanceof ArrayType) {
            return new SliceListOfArrayOp(type);
        } else if (elementType instanceof ListType) {
            return new SliceListOfListOp(type);
        } else if (elementType instanceof TupleType) {
            return new SliceListOfTupleOp(type);
        }
        return null;
    }

    @Override
    public Object getKey() {
        return originalType;
    }
}
