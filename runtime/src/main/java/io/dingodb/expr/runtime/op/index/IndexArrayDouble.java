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

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class IndexArrayDouble extends IndexOp {
    public static final IndexArrayDouble INSTANCE = new IndexArrayDouble();

    private static final long serialVersionUID = -801702592952722400L;

    private IndexArrayDouble() {
        super(Types.ARRAY_DOUBLE);
    }

    @Override
    protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        return ((double[]) value0)[(int) value1];
    }
}
