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

package io.dingodb.expr.runtime.op.special;

import io.dingodb.expr.annotations.Operators;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators(nullable = true)
abstract class IsFalseFun extends SpecialFun {
    public static final String NAME = "IS_FALSE";

    private static final long serialVersionUID = -3329238486794004799L;

    static boolean isFalse(Integer value) {
        return value != null && value == 0;
    }

    static boolean isFalse(Long value) {
        return value != null && value == 0;
    }

    static boolean isFalse(Float value) {
        return value != null && value == 0;
    }

    static boolean isFalse(Double value) {
        return value != null && value == 0;
    }

    static boolean isFalse(Boolean value) {
        return value != null && !value;
    }

    static boolean isFalse(BigDecimal value) {
        return value != null && value.compareTo(BigDecimal.ZERO) == 0;
    }

    static boolean isFalse(String value) {
        return value != null;
    }

    static boolean isFalse(byte[] value) {
        return value != null;
    }

    static boolean isFalse(Date value) {
        return false;
    }

    static boolean isFalse(Time value) {
        return false;
    }

    static boolean isFalse(Timestamp value) {
        return false;
    }

    static boolean isFalse(Void ignoredValue) {
        return false;
    }

    @Override
    public final @NonNull String getName() {
        return NAME;
    }
}
