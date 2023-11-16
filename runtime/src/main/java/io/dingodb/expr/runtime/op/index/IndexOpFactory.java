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
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class IndexOpFactory extends BinaryOp {
    public static final IndexOpFactory INSTANCE = new IndexOpFactory();

    private static final long serialVersionUID = 4166202157298784594L;

    @Override
    public IndexOpFactory getOp(Object key) {
        if (key != null) {
            if (key instanceof ArrayType) {
                return IndexArrayOp.of((ArrayType) key);
            } else if (key instanceof ListType) {
                return IndexListOp.of((ListType) key);
            } else if (key instanceof MapType) {
                return IndexMapOp.of((MapType) key);
            } else if (key instanceof TupleType) {
                return IndexTupleOp.of((TupleType) key);
            }
        }
        return null;
    }

    @Override
    public final @NonNull OpType getOpType() {
        return OpType.INDEX;
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
        if (types[0] instanceof CollectionType || types[0] instanceof TupleType) {
            types[1] = Types.INT;
            return types[0];
        } else if (types[0] instanceof MapType) {
            types[1] = ((MapType) types[0]).getKeyType();
            return types[0];
        }
        return null;
    }
}
