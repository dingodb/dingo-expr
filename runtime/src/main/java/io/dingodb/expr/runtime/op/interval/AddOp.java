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
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
import io.dingodb.expr.common.type.IntervalDayTimeType;
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalHourType;
import io.dingodb.expr.common.type.IntervalMinuteType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalQuarterType;
import io.dingodb.expr.common.type.IntervalSecondType;
import io.dingodb.expr.common.type.IntervalType;
import io.dingodb.expr.common.type.IntervalWeekType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.common.utils.CastWithString;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryIntervalOp;
import io.dingodb.expr.runtime.op.OpType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;

@Operators
public class AddOp extends BinaryIntervalOp {

    private static final long serialVersionUID = -3920946411706267558L;

    static Date add(Date value0, IntervalYearType.IntervalYear value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.DATE);

        long amount;
        ChronoUnit unit;
        if (value1.elementType instanceof IntervalMonthType) {
            unit = ChronoUnit.MONTHS;
            amount = value1.value.longValue();
        } else {
            unit = ChronoUnit.YEARS;
            amount = value1.value.intValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);
        return (Date) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.DATE);
    }

    static Date add(Date value0, IntervalMonthType.IntervalMonth value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.DATE);

        long amount;
        ChronoUnit unit;
        if (value1.elementType instanceof IntervalQuarterType) {
            unit = ChronoUnit.MONTHS;
            amount = value1.value.longValue() * 3;
        } else {
            unit = ChronoUnit.MONTHS;
            amount = value1.value.intValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);
        return (Date) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.DATE);
    }

    static Date add(Date value0, IntervalDayType.IntervalDay value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.DATE);

        long amount;
        ChronoUnit unit = ChronoUnit.DAYS;

        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (24 * 60 * 60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);
        return (Date) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.DATE);
    }

    static Date add(Date value0, IntervalWeekType.IntervalWeek value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.DATE);

        long amount;
        ChronoUnit unit = ChronoUnit.WEEKS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (60 * 60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Date) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.DATE);
    }

    static Timestamp add(Date value0, IntervalHourType.IntervalHour value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.HOURS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (60 * 60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Timestamp add(Date value0, IntervalMinuteType.IntervalMinute value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.MINUTES;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Timestamp add(Date value0, IntervalSecondType.IntervalSecond value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.SECONDS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / 1000;
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Time add(Time value0, IntervalHourType.IntervalHour value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIME);

        long amount;
        ChronoUnit unit = ChronoUnit.HOURS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (60 * 60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Time) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIME);
    }

    static Time add(Time value0, IntervalMinuteType.IntervalMinute value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIME);

        long amount;
        ChronoUnit unit = ChronoUnit.MINUTES;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Time) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIME);
    }

    static Time add(Time value0, IntervalSecondType.IntervalSecond value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIME);

        long amount;
        ChronoUnit unit = ChronoUnit.SECONDS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / 1000;
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Time) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIME);
    }

    static Timestamp add(Timestamp value0, IntervalYearType.IntervalYear value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit;
        if (value1.elementType instanceof IntervalMonthType) {
            unit = ChronoUnit.MONTHS;
            amount = value1.value.longValue();
        } else {
            unit = ChronoUnit.YEARS;
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Timestamp add(Timestamp value0, IntervalMonthType.IntervalMonth value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.MONTHS;
        if (value1.elementType instanceof IntervalQuarterType) {
            amount = value1.value.longValue() * 3;
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Timestamp add(Timestamp value0, IntervalDayType.IntervalDay value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.DAYS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (24 * 60 * 60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Timestamp add(Timestamp value0, IntervalWeekType.IntervalWeek value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.WEEKS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (60 * 60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Timestamp add(Timestamp value0, IntervalHourType.IntervalHour value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.HOURS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (60 * 60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);

        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Timestamp add(Timestamp value0, IntervalMinuteType.IntervalMinute value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.MINUTES;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / (60 * 1000);
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);
        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    static Timestamp add(Timestamp value0, IntervalSecondType.IntervalSecond value1, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);

        long amount;
        ChronoUnit unit = ChronoUnit.SECONDS;
        if (value1.elementType instanceof IntervalDayTimeType) {
            amount = value1.value.longValue() / 1000;
        } else {
            amount = value1.value.longValue();
        }
        DingoDateTime dateTime = processor.dateAdd(input, amount, unit);
        return (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
    }

    @Override
    public @NonNull OpType getOpType() {
        return OpType.ADD;
    }
}
