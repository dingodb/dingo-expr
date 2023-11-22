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

package io.dingodb.expr.runtime.type;

import org.checkerframework.checker.nullness.qual.NonNull;

public interface Type {
    int NOT_NUMERIC = 100;

    boolean isScalar();

    default int numericPrecedence() {
        return NOT_NUMERIC;
    }

    default boolean isNumeric() {
        return numericPrecedence() != NOT_NUMERIC;
    }

    default boolean isCompatible(@NonNull Type type) {
        // Types.NULL can be converted to any type.
        return type.equals(this) || type.equals(Types.NULL);
    }

    <R, T> R accept(@NonNull TypeVisitor<R, T> visitor, T obj);
}
