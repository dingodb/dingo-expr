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
import io.dingodb.expr.runtime.op.BinarySpecialOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
abstract class IfNullFun extends BinarySpecialOp {
    public static final String NAME = "IFNULL";
    private static final long serialVersionUID = 203255994751318689L;

    static Integer ifNull(Integer value0, Integer value1) {
        return value0 == null ? value1 : value0;
    }

    static Long ifNull(Long value0, Long value1) {
        return value0 == null ? value1 : value0;
    }

    static Float ifNull(Float value0, Float value1) {
        return value0 == null ? value1 : value0;
    }

    static Double ifNull(Double value0, Double value1) {
        return value0 == null ? value1 : value0;
    }

    static BigDecimal ifNull(BigDecimal value0, BigDecimal value1) {
        return value0 == null ? value1 : value0;
    }

    static Boolean ifNull(Boolean value0, Boolean value1) {
        return value0 == null ? value1 : value0;
    }

    static String ifNull(String value0, String value1) {
        return value0 == null ? value1 : value0;
    }

    static byte[] ifNull(byte[] value0, byte[] value1) {
        return value0 == null ? value1 : value0;
    }

    static Date ifNull(Date value0, Date value1) {
        return value0 == null ? value1 : value0;
    }

    static Time ifNull(Time value0, Time value1) {
        return value0 == null ? value1 : value0;
    }

    static Timestamp ifNull(Timestamp value0, Timestamp value1) {
        return value0 == null ? value1 : value0;
    }

    static Object ifNull(Object value0, Object value1) {
        if (value0 instanceof Float && (value1 instanceof Double || value1 instanceof BigDecimal)) {
            return new BigDecimal(value0.toString()).doubleValue();
        }
        return value0 == null ? value1 : value0;
    }

    static @Nullable Object ifNull(Void value0, Void value1) {
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
