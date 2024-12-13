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

package io.dingodb.expr.coding;

import io.dingodb.expr.common.type.IntType;
import io.dingodb.expr.common.type.TypeVisitorBase;
import io.dingodb.expr.runtime.utils.CodecUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.OutputStream;
import java.lang.reflect.Array;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class ArrayCoder extends TypeVisitorBase<CodingFlag, @NonNull OutputStream> {
    private static final byte ARRAY = (byte) 0x60;

    private final Object array;

    @SneakyThrows
    @Override
    public CodingFlag visitIntType(@NonNull IntType type, @NonNull OutputStream obj) {
        obj.write(ARRAY | TypeCoder.TYPE_INT32);
        int len = Array.getLength(array);
        CodecUtils.encodeVarInt(obj, len);
        for (int i = 0; i < len; ++i) {
            CodecUtils.encodeVarInt(obj, Array.getInt(array, i));
        }
        return CodingFlag.OK;
    }
}
