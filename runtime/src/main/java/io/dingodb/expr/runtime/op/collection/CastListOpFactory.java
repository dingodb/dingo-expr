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

import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.cast.CastOp;
import io.dingodb.expr.runtime.type.ArrayType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CastListOpFactory extends CastCollectionOpFactory {
    private static final long serialVersionUID = -2079958375710881440L;

    @Getter
    protected final ListType type;
    @Getter
    protected final ListType key;

    public CastListOpFactory(CastOp castOp) {
        super(castOp);
        type = Types.list(castOp.getType());
        Object k = castOp.getKey();
        key = (k != null ? Types.list((Type) k) : null);
    }

    @Override
    public @NonNull String getName() {
        return ListConstructorOpFactory.NAME + "_" + castOp.getName();
    }

    @Override
    public UnaryOp getOp(Object key) {
        if (key instanceof ArrayType) {
            return new CastListArrayOp(getCastOp((ArrayType) key));
        } else if (key instanceof ListType) {
            return new CastListListOp(getCastOp((ListType) key));
        }
        return null;
    }
}
