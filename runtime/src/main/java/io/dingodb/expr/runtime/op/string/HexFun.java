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

package io.dingodb.expr.runtime.op.string;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.utils.CodecUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

@Operators
abstract class HexFun extends UnaryStringFun {
    public static final String NAME = "HEX";

    private static final long serialVersionUID = 7550168473179742157L;

    static byte @NonNull [] hex(@NonNull String value) {
        return CodecUtils.hexStringToBytes(value);
    }

    @Override
    public final Type getType() {
        return Types.BYTES;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
