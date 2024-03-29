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

package io.dingodb.expr.runtime.op.relational;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.op.OpType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
abstract class GtOp extends RelationalOp {
    private static final long serialVersionUID = -743665850736586356L;

    static boolean gt(int value0, int value1) {
        return value0 > value1;
    }

    static boolean gt(long value0, long value1) {
        return value0 > value1;
    }

    static boolean gt(float value0, float value1) {
        return value0 > value1;
    }

    static boolean gt(double value0, double value1) {
        return value0 > value1;
    }

    static boolean gt(boolean value0, boolean value1) {
        return value0 && !value1;
    }

    static boolean gt(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.compareTo(value1) > 0;
    }

    static boolean gt(@NonNull String value0, String value1) {
        return value0.compareTo(value1) > 0;
    }

    static boolean gt(@NonNull Date value0, Date value1) {
        return value0.after(value1);
    }

    static boolean gt(@NonNull Time value0, Time value1) {
        return value0.after(value1);
    }

    static boolean gt(@NonNull Timestamp value0, Timestamp value1) {
        return value0.after(value1);
    }

    @Override
    public final @NonNull OpType getOpType() {
        return OpType.GT;
    }
}
