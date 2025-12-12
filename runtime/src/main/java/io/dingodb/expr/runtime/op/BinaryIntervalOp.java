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

package io.dingodb.expr.runtime.op;

import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalHourType;
import io.dingodb.expr.common.type.IntervalMinuteType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalSecondType;
import io.dingodb.expr.common.type.IntervalType;
import io.dingodb.expr.common.type.IntervalWeekType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.common.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;

public abstract class BinaryIntervalOp extends BinaryOp {

    private static final long serialVersionUID = -4216390313132926199L;

    @Override
    public @Nullable OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return OpKeys.INTERVAL.keyOf(type0, type1);
    }

    public static IntervalType buildInterval(BigDecimal value0, IntervalType value1) {
        if (value1 instanceof IntervalYearType.IntervalYear) {
            return new IntervalYearType.IntervalYear(value0.multiply(new BigDecimal(12)),
                ((IntervalYearType.IntervalYear) value1).elementType);
        } else if (value1 instanceof IntervalMonthType.IntervalMonth) {
            return new IntervalMonthType.IntervalMonth(value0, null);
        } else if (value1 instanceof IntervalDayType.IntervalDay) {
            return new IntervalDayType.IntervalDay(value0.multiply(new BigDecimal(24 * 60 * 60 * 1000)),
                ((IntervalDayType.IntervalDay) value1).elementType
            );
        } else if (value1 instanceof IntervalWeekType.IntervalWeek) {
            return new IntervalWeekType.IntervalWeek(value0.multiply(new BigDecimal(60 * 60 * 1000)),
                ((IntervalWeekType.IntervalWeek) value1).elementType
            );
        } else if (value1 instanceof IntervalHourType.IntervalHour) {
            return new IntervalHourType.IntervalHour(value0.multiply(new BigDecimal(60 * 60 * 1000)),
                ((IntervalHourType.IntervalHour) value1).elementType
            );
        } else if (value1 instanceof IntervalMinuteType.IntervalMinute) {
            return new IntervalHourType.IntervalHour(value0.multiply(new BigDecimal(60 * 1000)),
                ((IntervalMinuteType.IntervalMinute) value1).elementType
            );
        } else if (value1 instanceof IntervalSecondType.IntervalSecond) {
            return new IntervalSecondType.IntervalSecond(value0.multiply(new BigDecimal(1000)),
                ((IntervalSecondType.IntervalSecond) value1).elementType
            );
        }
        return null;
    }
}
