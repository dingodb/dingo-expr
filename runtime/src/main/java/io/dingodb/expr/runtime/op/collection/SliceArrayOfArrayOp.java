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
import io.dingodb.expr.common.type.Types;

import java.lang.reflect.Array;

public final class SliceArrayOfArrayOp extends SliceArrayOp {
    private static final long serialVersionUID = -580934004823081222L;

    SliceArrayOfArrayOp(ArrayType originalType) {
        super(originalType, Types.array(((ArrayType) originalType.getElementType()).getElementType()));
    }

    @Override
    Object getValueOf(Object value, int index) {
        return Array.get(value, index);
    }
}
