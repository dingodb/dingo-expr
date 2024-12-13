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
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.cast.CastOp;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CastArrayOpFactory extends CastCollectionOpFactory {
    private static final long serialVersionUID = 8968772411673886443L;

    @Getter
    protected final ArrayType type;
    @Getter
    protected final ArrayType key;

    public CastArrayOpFactory(CastOp castOp) {
        super(castOp);
        type = Types.array(castOp.getType());
        OpKey k = castOp.getKey();
        key = (k != null ? Types.array((Type) k) : null);
    }

    @Override
    public @NonNull String getName() {
        return ArrayConstructorOpFactory.NAME + "_" + castOp.getName();
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        if (key instanceof ArrayType) {
            return new CastArrayArrayOp(getCastOp((ArrayType) key));
        } else if (key instanceof ListType) {
            return new CastArrayListOp(getCastOp((ListType) key));
        }
        return null;
    }
}
