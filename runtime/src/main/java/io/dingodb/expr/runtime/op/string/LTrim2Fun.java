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
import org.checkerframework.checker.nullness.qual.NonNull;

@Operators
abstract class LTrim2Fun extends BinaryStringStringFun {
    public static final String NAME = "LTRIM";

    private static final long serialVersionUID = 4010086278241913516L;

    static @NonNull String ltrim(@NonNull String value0, @NonNull String value1) {
        int result = -1;
        int len = value0.length();
        int len1 = value1.length();
        int index = 0;
        while (value0.startsWith(value1, index) && index < len) {
            index += len1;
            result = index;
        }
        return result >= 0 ? value0.substring(result) : value0;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
