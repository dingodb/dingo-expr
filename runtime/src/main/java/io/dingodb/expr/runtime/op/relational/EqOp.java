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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

@Operators
abstract class EqOp extends RelationalOp {
    private static final long serialVersionUID = -2332942383399753798L;

    static boolean eq(int value0, int value1) {
        return value0 == value1;
    }

    static boolean eq(long value0, long value1) {
        return value0 == value1;
    }

    static boolean eq(float value0, float value1) {
        return value0 == value1;
    }

    static boolean eq(double value0, double value1) {
        return value0 == value1;
    }

    static boolean eq(boolean value0, boolean value1) {
        return value0 == value1;
    }

    static boolean eq(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.compareTo(value1) == 0;
    }

    static boolean eq(@NonNull String value0, String value1) {
        return value0.equals(value1);
    }

    static boolean eq(byte[] value0, byte[] value1) {
        return Arrays.equals(value0, value1);
    }

    static boolean eq(@NonNull Date value0, Object value1) {
        if (value1 instanceof Date) {
            return value0.equals((Date)value1);
        } else if (value1 instanceof Timestamp) {
            LocalDateTime localDateTime = ((Timestamp)value1).toLocalDateTime();
            Date dateValue1 = new Date(localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
            return value0.equals(dateValue1);
        }
        return false;
    }

    static boolean eq(@NonNull Time value0, Time value1) {
        return value0.equals(value1);
    }

    static boolean eq(@NonNull Timestamp value0, Object value1) {
        if (value1 instanceof Timestamp) {
            return value0.equals((Timestamp)value1);
        } else if (value1 instanceof Date) {
            LocalDateTime localDateTime = value0.toLocalDateTime();
            Date dateValue0 = new Date(localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
            return dateValue0.equals(value1);
        }
        return false;
    }

    @Override
    public final @NonNull OpType getOpType() {
        return OpType.EQ;
    }
}
