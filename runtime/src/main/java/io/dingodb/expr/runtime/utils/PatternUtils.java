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

package io.dingodb.expr.runtime.utils;

import org.checkerframework.checker.nullness.qual.NonNull;

public final class PatternUtils {
    private PatternUtils() {
    }

    public static @NonNull String convertSqlToRegex(@NonNull String pattern, char escape) {
        StringBuilder b = new StringBuilder();
        int len = pattern.length();
        for (int i = 0; i < len; ++i) {
            char c = pattern.charAt(i);
            if (c == escape) {
                ++i;
                if (i < len) {
                    c = pattern.charAt(i);
                    if (c == '%' || c == '_') {
                        b.append(c);
                    } else {
                        b.append('\\').append(c);
                    }
                } else {
                    b.append(c);
                    break;
                }
            } else if (c == '%') {
                b.append(".*");
            } else if (c == '_') {
                b.append(".");
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }
}
