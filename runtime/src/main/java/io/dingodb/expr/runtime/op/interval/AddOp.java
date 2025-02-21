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
import io.dingodb.expr.common.type.IntervalHourType;
import io.dingodb.expr.common.type.IntervalMinuteType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalSecondType;
import io.dingodb.expr.common.type.IntervalWeekType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.runtime.op.BinaryIntervalOp;
import io.dingodb.expr.runtime.op.OpType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Operators
public class AddOp extends BinaryIntervalOp {

    private static final long serialVersionUID = -3920946411706267558L;

    static Date add(Date value0, IntervalYearType.IntervalYear value1) {
        LocalDate localDate = value0.toLocalDate();
        LocalDate resultDate;
        if (value1.elementType instanceof IntervalMonthType) {
            resultDate = localDate.plusMonths(value1.value.intValue());
        } else {
            resultDate = localDate.plusYears(value1.value.intValue());
        }
        return Date.valueOf(resultDate);
    }

    static Date add(Date value0, IntervalMonthType.IntervalMonth value1) {
        LocalDate localDate = value0.toLocalDate();
        LocalDate resultDate = localDate.plusMonths(value1.value.intValue());
        return Date.valueOf(resultDate);
    }

    static Date add(Date value0, IntervalDayType.IntervalDay value1) {
        LocalDate localDate = value0.toLocalDate();
        long daysToAdd;
        if (value1.elementType instanceof IntervalDayTimeType) {
            daysToAdd = value1.value.longValue() / (24 * 60 * 60 * 1000);
        } else {
            daysToAdd = value1.value.longValue();
        }
        LocalDate resultDate = localDate.plusDays(daysToAdd);
        return Date.valueOf(resultDate);
    }

    static Date add(Date value0, IntervalWeekType.IntervalWeek value1) {
        LocalDate localDate = value0.toLocalDate();
        long week;
        if (value1.elementType instanceof IntervalDayTimeType) {
            week = value1.value.longValue() / (60 * 60 * 1000);
        } else {
            week = value1.value.longValue();
        }
        LocalDate resultDate = localDate.plusWeeks(week);
        return Date.valueOf(resultDate);
    }

    static Timestamp add(Timestamp value0, IntervalYearType.IntervalYear value1) {
        LocalDateTime localDateTime = value0.toLocalDateTime();
        LocalDateTime resultDateTime;
        if (value1.elementType instanceof IntervalMonthType) {
            resultDateTime = localDateTime.plusMonths(value1.value.longValue());
        } else {
            resultDateTime = localDateTime.plusYears(value1.value.longValue());
        }
        return Timestamp.valueOf(resultDateTime);
    }

    static Timestamp add(Timestamp value0, IntervalMonthType.IntervalMonth value1) {
        LocalDateTime localDateTime = value0.toLocalDateTime();
        LocalDateTime resultDateTime = localDateTime.plusMonths(value1.value.longValue());
        return Timestamp.valueOf(resultDateTime);
    }

    static Timestamp add(Timestamp value0, IntervalDayType.IntervalDay value1) {
        LocalDateTime localDateTime = value0.toLocalDateTime();
        long daysToAdd;
        if (value1.elementType instanceof IntervalDayTimeType) {
            daysToAdd = value1.value.longValue() / (24 * 60 * 60 * 1000);
        } else {
            daysToAdd = value1.value.longValue();
        }
        LocalDateTime resultDateTime = localDateTime.plusDays(daysToAdd);
        return Timestamp.valueOf(resultDateTime);
    }

    static Timestamp add(Timestamp value0, IntervalWeekType.IntervalWeek value1) {
        LocalDateTime localDateTime = value0.toLocalDateTime();
        long week;
        if (value1.elementType instanceof IntervalDayTimeType) {
            week = value1.value.longValue() / (60 * 60 * 1000);
        } else {
            week = value1.value.longValue();
        }
        LocalDateTime resultDateTime = localDateTime.plusWeeks(week);
        return Timestamp.valueOf(resultDateTime);
    }

    static Timestamp add(Timestamp value0, IntervalHourType.IntervalHour value1) {
        LocalDateTime localDateTime = value0.toLocalDateTime();
        long hours;
        if (value1.elementType instanceof IntervalDayTimeType) {
            hours = value1.value.longValue() / (60 * 60 * 1000);
        } else {
            hours = value1.value.longValue();
        }
        LocalDateTime resultDateTime = localDateTime.plusHours(hours);
        return Timestamp.valueOf(resultDateTime);
    }

    static Timestamp add(Timestamp value0, IntervalMinuteType.IntervalMinute value1) {
        LocalDateTime localDateTime = value0.toLocalDateTime();
        long minute;
        if (value1.elementType instanceof IntervalDayTimeType) {
            minute = value1.value.longValue() / (60 * 1000);
        } else {
            minute = value1.value.longValue();
        }
        LocalDateTime resultDateTime = localDateTime.plusMinutes(minute);
        return Timestamp.valueOf(resultDateTime);
    }

    static Timestamp add(Timestamp value0, IntervalSecondType.IntervalSecond value1) {
        LocalDateTime localDateTime = value0.toLocalDateTime();
        long second;
        if (value1.elementType instanceof IntervalDayTimeType) {
            second = value1.value.longValue() / 1000;
        } else {
            second = value1.value.longValue();
        }
        LocalDateTime resultDateTime = localDateTime.plusSeconds(second);
        return Timestamp.valueOf(resultDateTime);
    }

    @Override
    public @NonNull OpType getOpType() {
        return OpType.ADD;
    }
}
