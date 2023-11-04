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

@Operators(nullable = true)
abstract class Mid3Fun extends TertiaryStringIntIntFun {
    public static final String NAME = "MID";

    private static final long serialVersionUID = -1474048641131723710L;

    static String mid(String value, Integer start, Integer count) {
        if (start != null && count != null) {
            if (value != null && start != 0 && count > 0) {
                int len = value.length();
                // `start` begins at 1.
                if (start < 0) {
                    start += len;
                } else {
                    start -= 1;
                }
                int end = start + count;
                return end >= len ? value.substring(start) : value.substring(start, end);
            }
            return "";
        }
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
