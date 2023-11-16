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
import io.dingodb.expr.runtime.type.CollectionType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.Types;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

class SliceArrayOp extends SliceOpFactory {
    private static final long serialVersionUID = 6362732903056529224L;

    protected final ArrayType originalType;
    @Getter
    protected final ArrayType type;

    protected SliceArrayOp(@NonNull ArrayType originalType) {
        this.originalType = originalType;
        this.type = Types.array(((CollectionType) originalType.getElementType()).getElementType());
    }

    public static @Nullable SliceArrayOp of(ArrayType type) {
        if (type.getElementType() instanceof ArrayType) {
            return new SliceArrayOfArrayOp(type);
        } else if (type.getElementType() instanceof ListType) {
            return new SliceArrayOfListOp(type);
        }
        return null;
    }

    @Override
    public Object getKey() {
        return originalType;
    }
}
