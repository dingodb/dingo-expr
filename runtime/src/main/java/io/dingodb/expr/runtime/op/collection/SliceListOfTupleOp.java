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
import io.dingodb.expr.runtime.type.ArrayType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public final class SliceListOfTupleOp extends SliceListOp {
    private static final long serialVersionUID = -580934004823081222L;

    SliceListOfTupleOp(ListType originalType) {
        super(originalType);
    }

    @Override
    protected @NonNull Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        int index = (Integer) value1;
        List<?> list = (List<?>) value0;
        int size = list.size();
        List<Object> result = new ArrayList<>(size);
        for (Object element : list) {
            result.add(Array.get(element, index));
        }
        return result;
    }

    @Override
    public Type getType() {
        return Types.LIST_ANY;
    }
}
