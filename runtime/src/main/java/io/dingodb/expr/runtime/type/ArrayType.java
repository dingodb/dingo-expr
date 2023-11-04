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

public final class ArrayType extends CollectionType {
    public static final String NAME = "ARRAY";

    private static final int CODE = 1001;

    ArrayType(Type elementType) {
        super(elementType);
    }

    @Override
    public <R, T> R accept(@NonNull TypeVisitor<R, T> visitor, T obj) {
        return visitor.visitArrayType(this, obj);
    }

    @Override
    public int hashCode() {
        return CODE * 31 + elementType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ArrayType
            && elementType.equals(((ArrayType) obj).elementType);
    }

    @Override
    public String toString() {
        return NAME + "<" + elementType + ">";
    }
}
