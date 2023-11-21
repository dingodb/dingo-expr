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

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.cast.CastOp;

import java.lang.reflect.Array;

public final class CastArrayArrayOp extends CastArrayOpFactory {
    private static final long serialVersionUID = 1060485143951972916L;

    CastArrayArrayOp(CastOp castOp) {
        super(castOp);
    }

    @Override
    public Object evalValue(Object value, ExprConfig config) {
        int size = Array.getLength(value);
        Object array = ArrayBuilder.INSTANCE.visit(type.getElementType(), size);
        for (int i = 0; i < size; ++i) {
            Object v = castOp.evalValue(Array.get(value, i), config);
            Array.set(array, i, v);
        }
        return array;
    }
}
