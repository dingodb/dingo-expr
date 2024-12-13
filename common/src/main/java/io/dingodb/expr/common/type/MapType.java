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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class MapType implements Type {
    public static final String NAME = "MAP";

    private static final int CODE = 1003;

    @Getter
    private final Type keyType;
    @Getter
    private final Type valueType;

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public <R, T> R accept(@NonNull TypeVisitor<R, T> visitor, T obj) {
        return visitor.visitMapType(this, obj);
    }

    @Override
    public int hashCode() {
        return CODE * 31 * 31 + keyType.hashCode() * 31 + valueType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof MapType
               && keyType.equals(((MapType) obj).keyType)
               && valueType.equals(((MapType) obj).valueType);
    }

    @Override
    public @NonNull String toString() {
        return NAME + "<" + keyType + ", " + valueType + ">";
    }
}
