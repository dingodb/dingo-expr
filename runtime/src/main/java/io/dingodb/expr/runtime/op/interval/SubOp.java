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

package io.dingodb.expr.runtime.op.interval;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.common.type.IntervalDayTimeType;
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.runtime.op.BinaryIntervalOp;
import io.dingodb.expr.runtime.op.OpType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.time.LocalDate;

@Operators
public abstract class SubOp extends BinaryIntervalOp {

    private static final long serialVersionUID = 5390298840584347195L;

    static Date sub(Date value0, IntervalYearType.IntervalYear value1) {
        LocalDate localDate = value0.toLocalDate();
        LocalDate resultDate;
        if (value1.elementType instanceof IntervalMonthType) {
            resultDate = localDate.minusMonths(value1.value.intValue());
        } else {
            resultDate = localDate.minusYears(value1.value.intValue());
        }
        return Date.valueOf(resultDate);
    }

    static Date sub(Date value0, IntervalMonthType.IntervalMonth value1) {
        LocalDate localDate = value0.toLocalDate();
        LocalDate resultDate = localDate.minusMonths(value1.value.intValue());
        return Date.valueOf(resultDate);
    }

    static Date sub(Date value0, IntervalDayType.IntervalDay value1) {
        LocalDate localDate = value0.toLocalDate();
        long daysToSubtract;
        if (value1.elementType instanceof IntervalDayTimeType) {
            daysToSubtract = value1.value.longValue() / (24 * 60 * 60 * 1000);
        } else {
            daysToSubtract = value1.value.longValue();
        }
        LocalDate resultDate = localDate.minusDays(daysToSubtract);
        return Date.valueOf(resultDate);
    }

    @Override
    public @NonNull OpType getOpType() {
        return OpType.SUB;
    }
}
