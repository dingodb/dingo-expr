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
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalHourType;
import io.dingodb.expr.common.type.IntervalMinuteType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalSecondType;
import io.dingodb.expr.common.type.IntervalType;
import io.dingodb.expr.common.type.IntervalWeekType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.common.utils.CastWithString;
import io.dingodb.expr.runtime.op.BinaryIntervalOp;
import io.dingodb.expr.runtime.op.OpType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
public class MulOp extends BinaryIntervalOp {

    private static final long serialVersionUID = -1357285098612662070L;

    static IntervalYearType.IntervalYear mul(String value0, IntervalYearType.IntervalYear value1) {
        BigDecimal decimal = new BigDecimal(CastWithString.longCastWithStringCompat(value0.split("\\.")[0]));
        return (IntervalYearType.IntervalYear) buildInterval(decimal, value1);
    }

    static IntervalMonthType.IntervalMonth mul(String value0, IntervalMonthType.IntervalMonth value1) {
        BigDecimal decimal = new BigDecimal(CastWithString.longCastWithStringCompat(value0.split("\\.")[0]));
        return (IntervalMonthType.IntervalMonth) buildInterval(decimal, value1);
    }

    static IntervalDayType.IntervalDay mul(String value0, IntervalDayType.IntervalDay value1) {
        BigDecimal decimal = new BigDecimal(CastWithString.longCastWithStringCompat(value0.split("\\.")[0]));
        return (IntervalDayType.IntervalDay) buildInterval(decimal, value1);
    }

    static IntervalWeekType.IntervalWeek mul(String value0, IntervalWeekType.IntervalWeek value1) {
        BigDecimal decimal = new BigDecimal(CastWithString.longCastWithStringCompat(value0.split("\\.")[0]));
        return (IntervalWeekType.IntervalWeek) buildInterval(decimal, value1);
    }

    static IntervalHourType.IntervalHour mul(String value0, IntervalHourType.IntervalHour value1) {
        BigDecimal decimal = new BigDecimal(CastWithString.longCastWithStringCompat(value0.split("\\.")[0]));
        return (IntervalHourType.IntervalHour) buildInterval(decimal, value1);
    }

    static IntervalMinuteType.IntervalMinute mul(String value0, IntervalMinuteType.IntervalMinute value1) {
        BigDecimal decimal = new BigDecimal(CastWithString.longCastWithStringCompat(value0.split("\\.")[0]));
        return (IntervalMinuteType.IntervalMinute) buildInterval(decimal, value1);
    }

    static IntervalSecondType.IntervalSecond mul(String value0, IntervalSecondType.IntervalSecond value1) {
        BigDecimal decimal = new BigDecimal(CastWithString.longCastWithStringCompat(value0.split("\\.")[0]));
        return (IntervalSecondType.IntervalSecond) buildInterval(decimal, value1);
    }

    static IntervalYearType.IntervalYear mul(Integer value0, IntervalYearType.IntervalYear value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalYearType.IntervalYear) buildInterval(decimal, value1);
    }

    static IntervalMonthType.IntervalMonth mul(Integer value0, IntervalMonthType.IntervalMonth value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMonthType.IntervalMonth) buildInterval(decimal, value1);
    }

    static IntervalDayType.IntervalDay mul(Integer value0, IntervalDayType.IntervalDay value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalDayType.IntervalDay) buildInterval(decimal, value1);
    }

    static IntervalWeekType.IntervalWeek mul(Integer value0, IntervalWeekType.IntervalWeek value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalWeekType.IntervalWeek) buildInterval(decimal, value1);
    }

    static IntervalHourType.IntervalHour mul(Integer value0, IntervalHourType.IntervalHour value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalHourType.IntervalHour) buildInterval(decimal, value1);
    }

    static IntervalMinuteType.IntervalMinute mul(Integer value0, IntervalMinuteType.IntervalMinute value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMinuteType.IntervalMinute) buildInterval(decimal, value1);
    }

    static IntervalSecondType.IntervalSecond mul(Integer value0, IntervalSecondType.IntervalSecond value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalSecondType.IntervalSecond) buildInterval(decimal, value1);
    }

    static IntervalYearType.IntervalYear mul(Long value0, IntervalYearType.IntervalYear value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalYearType.IntervalYear) buildInterval(decimal, value1);
    }

    static IntervalMonthType.IntervalMonth mul(Long value0, IntervalMonthType.IntervalMonth value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMonthType.IntervalMonth) buildInterval(decimal, value1);
    }

    static IntervalDayType.IntervalDay mul(Long value0, IntervalDayType.IntervalDay value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalDayType.IntervalDay) buildInterval(decimal, value1);
    }

    static IntervalWeekType.IntervalWeek mul(Long value0, IntervalWeekType.IntervalWeek value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalWeekType.IntervalWeek) buildInterval(decimal, value1);
    }

    static IntervalHourType.IntervalHour mul(Long value0, IntervalHourType.IntervalHour value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalHourType.IntervalHour) buildInterval(decimal, value1);
    }

    static IntervalMinuteType.IntervalMinute mul(Long value0, IntervalMinuteType.IntervalMinute value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMinuteType.IntervalMinute) buildInterval(decimal, value1);
    }

    static IntervalSecondType.IntervalSecond mul(Long value0, IntervalSecondType.IntervalSecond value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalSecondType.IntervalSecond) buildInterval(decimal, value1);
    }

    static IntervalYearType.IntervalYear mul(Double value0, IntervalYearType.IntervalYear value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalYearType.IntervalYear) buildInterval(decimal, value1);
    }

    static IntervalMonthType.IntervalMonth mul(Double value0, IntervalMonthType.IntervalMonth value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMonthType.IntervalMonth) buildInterval(decimal, value1);
    }

    static IntervalDayType.IntervalDay mul(Double value0, IntervalDayType.IntervalDay value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalDayType.IntervalDay) buildInterval(decimal, value1);
    }

    static IntervalWeekType.IntervalWeek mul(Double value0, IntervalWeekType.IntervalWeek value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalWeekType.IntervalWeek) buildInterval(decimal, value1);
    }

    static IntervalHourType.IntervalHour mul(Double value0, IntervalHourType.IntervalHour value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalHourType.IntervalHour) buildInterval(decimal, value1);
    }

    static IntervalMinuteType.IntervalMinute mul(Double value0, IntervalMinuteType.IntervalMinute value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMinuteType.IntervalMinute) buildInterval(decimal, value1);
    }

    static IntervalSecondType.IntervalSecond mul(Double value0, IntervalSecondType.IntervalSecond value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalSecondType.IntervalSecond) buildInterval(decimal, value1);
    }

    static IntervalYearType.IntervalYear mul(Float value0, IntervalYearType.IntervalYear value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalYearType.IntervalYear) buildInterval(decimal, value1);
    }

    static IntervalMonthType.IntervalMonth mul(Float value0, IntervalMonthType.IntervalMonth value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMonthType.IntervalMonth) buildInterval(decimal, value1);
    }

    static IntervalDayType.IntervalDay mul(Float value0, IntervalDayType.IntervalDay value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalDayType.IntervalDay) buildInterval(decimal, value1);
    }

    static IntervalWeekType.IntervalWeek mul(Float value0, IntervalWeekType.IntervalWeek value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalWeekType.IntervalWeek) buildInterval(decimal, value1);
    }

    static IntervalHourType.IntervalHour mul(Float value0, IntervalHourType.IntervalHour value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalHourType.IntervalHour) buildInterval(decimal, value1);
    }

    static IntervalMinuteType.IntervalMinute mul(Float value0, IntervalMinuteType.IntervalMinute value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMinuteType.IntervalMinute) buildInterval(decimal, value1);
    }

    static IntervalSecondType.IntervalSecond mul(Float value0, IntervalSecondType.IntervalSecond value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalSecondType.IntervalSecond) buildInterval(decimal, value1);
    }

    static IntervalYearType.IntervalYear mul(Boolean value0, IntervalYearType.IntervalYear value1) {
        BigDecimal decimal = new BigDecimal(value0 ? 1 : 0);
        return (IntervalYearType.IntervalYear) buildInterval(decimal, value1);
    }

    static IntervalMonthType.IntervalMonth mul(Boolean value0, IntervalMonthType.IntervalMonth value1) {
        BigDecimal decimal = new BigDecimal(value0 ? 1 : 0);
        return (IntervalMonthType.IntervalMonth) buildInterval(decimal, value1);
    }

    static IntervalDayType.IntervalDay mul(Boolean value0, IntervalDayType.IntervalDay value1) {
        BigDecimal decimal = new BigDecimal(value0 ? 1 : 0);
        return (IntervalDayType.IntervalDay) buildInterval(decimal, value1);
    }

    static IntervalWeekType.IntervalWeek mul(Boolean value0, IntervalWeekType.IntervalWeek value1) {
        BigDecimal decimal = new BigDecimal(value0 ? 1 : 0);
        return (IntervalWeekType.IntervalWeek) buildInterval(decimal, value1);
    }

    static IntervalHourType.IntervalHour mul(Boolean value0, IntervalHourType.IntervalHour value1) {
        BigDecimal decimal = new BigDecimal(value0 ? 1 : 0);
        return (IntervalHourType.IntervalHour) buildInterval(decimal, value1);
    }

    static IntervalMinuteType.IntervalMinute mul(Boolean value0, IntervalMinuteType.IntervalMinute value1) {
        BigDecimal decimal = new BigDecimal(value0 ? 1 : 0);
        return (IntervalMinuteType.IntervalMinute) buildInterval(decimal, value1);
    }

    static IntervalSecondType.IntervalSecond mul(Boolean value0, IntervalSecondType.IntervalSecond value1) {
        BigDecimal decimal = new BigDecimal(value0 ? 1 : 0);
        return (IntervalSecondType.IntervalSecond) buildInterval(decimal, value1);
    }

    static IntervalYearType.IntervalYear mul(BigDecimal value0, IntervalYearType.IntervalYear value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalYearType.IntervalYear) buildInterval(decimal, value1);
    }

    static IntervalMonthType.IntervalMonth mul(BigDecimal value0, IntervalMonthType.IntervalMonth value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMonthType.IntervalMonth) buildInterval(decimal, value1);
    }

    static IntervalDayType.IntervalDay mul(BigDecimal value0, IntervalDayType.IntervalDay value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalDayType.IntervalDay) buildInterval(decimal, value1);
    }

    static IntervalWeekType.IntervalWeek mul(BigDecimal value0, IntervalWeekType.IntervalWeek value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalWeekType.IntervalWeek) buildInterval(decimal, value1);
    }

    static IntervalHourType.IntervalHour mul(BigDecimal value0, IntervalHourType.IntervalHour value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalHourType.IntervalHour) buildInterval(decimal, value1);
    }

    static IntervalMinuteType.IntervalMinute mul(BigDecimal value0, IntervalMinuteType.IntervalMinute value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalMinuteType.IntervalMinute) buildInterval(decimal, value1);
    }

    static IntervalSecondType.IntervalSecond mul(BigDecimal value0, IntervalSecondType.IntervalSecond value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return (IntervalSecondType.IntervalSecond) buildInterval(decimal, value1);
    }

    static IntervalType mul(Date value0, IntervalType value1) {
        return null;
    }

    static IntervalType mul(Time value0, IntervalType value1) {
        return null;
    }

    static IntervalType mul(Timestamp value0, IntervalType value1) {
        return null;
    }

    static IntervalType mul(Void value0, IntervalType value1) {
        return null;
    }

    @Override
    public @NonNull OpType getOpType() {
        return OpType.MUL;
    }
}
