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
import io.dingodb.expr.runtime.type.MapType;
import io.dingodb.expr.runtime.type.Type;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexMapOp extends IndexOpFactory {
    private static final long serialVersionUID = -5277892825485455044L;

    private final MapType originalType;

    public static @NonNull IndexMapOp of(MapType type) {
        return new IndexMapOp(type);
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        return ((Map<?, ?>) value0).get(value1);
    }

    @Override
    public Type getType() {
        return originalType.getValueType();
    }

    @Override
    public OpKey getKey() {
        return originalType;
    }
}
