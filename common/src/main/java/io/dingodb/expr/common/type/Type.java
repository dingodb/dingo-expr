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

package io.dingodb.expr.common.type;

import io.dingodb.expr.runtime.op.OpKey;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface Type extends OpKey {

    int NOT_NUMERIC = 100;

    boolean isScalar();

    default int numericPrecedence() {
        return NOT_NUMERIC;
    }

    default boolean isNumeric() {
        return numericPrecedence() != NOT_NUMERIC;
    }

    default boolean matches(@NonNull Object type) {
        // Types.NULL can be converted to any type.
        return equals(type) || Types.NULL.equals(type);
    }

    <R, T> R accept(@NonNull TypeVisitor<R, T> visitor, T obj);
}
