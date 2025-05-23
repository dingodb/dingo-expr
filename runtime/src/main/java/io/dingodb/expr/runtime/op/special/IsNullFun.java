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

//@Operators(nullable = true)
abstract class IsNullFun extends SpecialFun {
    public static final String NAME = "IS_NULL";

    private static final long serialVersionUID = -7962050514473926925L;

    static boolean isNull(Integer value) {
        return value == null;
    }

    static boolean isNull(Long value) {
        return value == null;
    }

    static boolean isNull(Float value) {
        return value == null;
    }

    static boolean isNull(Double value) {
        return value == null;
    }

    static boolean isNull(Boolean value) {
        return value == null;
    }

    static boolean isNull(BigDecimal value) {
        return value == null;
    }

    static boolean isNull(String value) {
        return value == null;
    }

    static boolean isNull(byte[] value) {
        return value == null;
    }

    static boolean isNull(Date value) {
        return value == null;
    }

    static boolean isNull(Time value) {
        return value == null;
    }

    static boolean isNull(Timestamp value) {
        return value == null;
    }

    static boolean isNull(Void ignoredValue) {
        return true;
    }

    @Override
    public final @NonNull String getName() {
        return NAME;
    }
}
