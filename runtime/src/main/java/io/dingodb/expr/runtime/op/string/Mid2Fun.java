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
abstract class Mid2Fun extends BinaryStringIntFun {
    public static final String NAME = "MID";

    private static final long serialVersionUID = 8598115032851767999L;

    static @NonNull String mid(@NonNull String value, int start) {
        int len = value.length();
        if (0 < start && start <= len) {
            start -= 1;
        } else if (-len <= start && start < 0) {
            start += len;
        } else {
            return "";
        }
        return value.substring(start);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
