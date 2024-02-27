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

import io.dingodb.expr.runtime.exception.NullElementsNotAllowed;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ExceptionUtils {
    private ExceptionUtils() {
    }

    public static @NonNull ArithmeticException exceedsIntRange() {
        return new ArithmeticException(
            "Value exceeds limits of INT, which is from "
            + Integer.MIN_VALUE + " to " + Integer.MAX_VALUE + "."
        );
    }

    public static @NonNull ArithmeticException exceedsLongRange() {
        return new ArithmeticException(
            "Value exceeds limits of LONG, which is from "
            + Long.MIN_VALUE + " to " + Long.MAX_VALUE + "."
        );
    }

    public static @NonNull Object nonNullElement(Object value) {
        if (value != null) {
            return value;
        }
        throw new NullElementsNotAllowed();
    }
}
