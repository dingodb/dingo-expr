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

package io.dingodb.expr.test;

import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

import static io.dingodb.expr.runtime.expr.Exprs.val;

public final class ExprsHelper {
    private ExprsHelper() {
    }

    public static @NonNull Val dec(Object value) {
        return val(value, Types.DECIMAL);
    }

    public static @NonNull Val bytes(Object value) {
        return val(value, Types.BYTES);
    }

    public static @NonNull Val date(Object value) {
        return val(value, Types.DATE);
    }

    public static @NonNull Val time(Object value) {
        return val(value, Types.TIME);
    }

    public static @NonNull Val ts(Object value) {
        return val(value, Types.TIMESTAMP);
    }

    public static long sec(long second) {
        return DateTimeUtils.fromSecond(second);
    }

    public static long sec(BigDecimal second) {
        return DateTimeUtils.fromSecond(second);
    }
}
