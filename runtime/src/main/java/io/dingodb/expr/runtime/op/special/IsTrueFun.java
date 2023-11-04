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
abstract class IsTrueFun extends SpecialFun {
    public static final String NAME = "IS_TRUE";

    private static final long serialVersionUID = 1004649937694829641L;

    static boolean isTrue(Integer value) {
        return value != null && value != 0;
    }

    static boolean isTrue(Long value) {
        return value != null && value != 0;
    }

    static boolean isTrue(Float value) {
        return value != null && value != 0;
    }

    static boolean isTrue(Double value) {
        return value != null && value != 0;
    }

    static boolean isTrue(Boolean value) {
        return value != null && value;
    }

    static boolean isTrue(BigDecimal value) {
        return value != null && value.compareTo(BigDecimal.ZERO) != 0;
    }

    static boolean isTrue(String value) {
        return false;
    }

    static boolean isTrue(byte[] value) {
        return false;
    }

    static boolean isTrue(Date value) {
        return value != null;
    }

    static boolean isTrue(Time value) {
        return value != null;
    }

    static boolean isTrue(Timestamp value) {
        return value != null;
    }

    static boolean isTrue(Void ignoredValue) {
        return false;
    }

    @Override
    public final @NonNull String getName() {
        return NAME;
    }
}
