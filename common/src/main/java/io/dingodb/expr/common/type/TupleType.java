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

import java.util.Arrays;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class TupleType implements Type {
    public static final String NAME = "TUPLE";

    private static final int CODE = 2002;

    @Getter
    private final Type[] types;

    public int getSize() {
        return types.length;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public <R, T> R accept(@NonNull TypeVisitor<R, T> visitor, T obj) {
        return visitor.visitTupleType(this, obj);
    }

    @Override
    public int hashCode() {
        int code = CODE;
        for (Type type : types) {
            code *= 31;
            code += type.hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TupleType
               && Arrays.equals(types, ((TupleType) obj).types);
    }

    @Override
    public String toString() {
        return NAME + Arrays.toString(types);
    }
}
