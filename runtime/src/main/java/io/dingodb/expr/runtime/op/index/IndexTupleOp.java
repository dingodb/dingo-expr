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
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.TupleType;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Array;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexTupleOp extends IndexOpFactory {
    private static final long serialVersionUID = -2166673591808125204L;

    private final TupleType originalType;

    public static @NonNull IndexTupleOp of(@NonNull TupleType type) {
        return new IndexTupleOp(type);
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        return Array.get(value0, (Integer) value1);
    }

    @Override
    public OpKey getKey() {
        return originalType;
    }
}
