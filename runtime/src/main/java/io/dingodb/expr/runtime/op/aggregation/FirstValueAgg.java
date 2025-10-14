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

package io.dingodb.expr.runtime.op.aggregation;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.ExprConfig;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serial;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class FirstValueAgg extends UnaryAgg {
    public static final String NAME = "FIRST_VALUE_AGG";

    public static final FirstValueAgg INSTANCE = new FirstValueAgg(null);

    @Serial
    private static final long serialVersionUID = -1444678046223537450L;

    @Getter
    private final Type type;

    @Override
    public Object first(@Nullable Object value, ExprConfig config) {
        return value;
    }

    @Override
    public Object add(@Nullable Object var, @Nullable Object value, ExprConfig config) {
        return value;
    }

    @Override
    public Object merge(@NonNull Object var1, @NonNull Object var2, ExprConfig config) {
        return null;
    }

    @Override
    public Object emptyValue() {
        return null;
    }
}
