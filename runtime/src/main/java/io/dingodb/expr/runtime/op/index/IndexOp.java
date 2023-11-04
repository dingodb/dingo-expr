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

import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.type.ArrayType;
import io.dingodb.expr.runtime.type.CollectionType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.MapType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class IndexOp extends BinaryOp {
    public static final IndexOp INSTANCE = new IndexOp(null);

    private static final long serialVersionUID = 4166202157298784594L;

    private final Type type;

    @Override
    public IndexOp getOp(Object key) {
        if (key == null) {
            return null;
        }
        Type type = (Type) key;
        if (type instanceof ArrayType) {
            return IndexArrayOpCreator.INSTANCE.visit(((ArrayType) type).getElementType());
        } else if (type instanceof ListType) {
            return new IndexList(type);
        } else if (type instanceof MapType) {
            return new IndexMap(type);
        }
        return null;
    }

    @Override
    public Type getType() {
        if (type instanceof ArrayType) {
            return ((ArrayType) type).getElementType();
        } else if (type instanceof MapType) {
            return ((MapType) type).getValueType();
        }
        return null;
    }

    @Override
    public final @NonNull OpType getOpType() {
        return OpType.INDEX;
    }

    @Override
    public Object getKey() {
        return !(type == null) ? type : super.getKey();
    }

    @Override
    public Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        if (type1.equals(Types.INT)) {
            return type0;
        }
        return null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        if (types[0] instanceof CollectionType) {
            types[1] = Types.INT;
        } else if (types[0] instanceof MapType) {
            types[1] = ((MapType) types[0]).getKeyType();
        } else {
            return null;
        }
        return types[0];
    }
}
