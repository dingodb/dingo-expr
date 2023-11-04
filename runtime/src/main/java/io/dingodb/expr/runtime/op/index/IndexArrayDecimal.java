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

import java.math.BigDecimal;

public final class IndexArrayDecimal extends IndexOp {
    public static final IndexArrayDecimal INSTANCE = new IndexArrayDecimal();

    private static final long serialVersionUID = 6174229709249283086L;

    private IndexArrayDecimal() {
        super(Types.ARRAY_DECIMAL);
    }

    @Override
    protected BigDecimal evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        return ((BigDecimal[]) value0)[(int) value1];
    }
}
