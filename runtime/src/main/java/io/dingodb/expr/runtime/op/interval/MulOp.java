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
import io.dingodb.expr.common.type.IntervalType;
import io.dingodb.expr.common.utils.CastWithString;
import io.dingodb.expr.runtime.op.BinaryIntervalOp;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
public class MulOp extends BinaryIntervalOp {

    private static final long serialVersionUID = -1357285098612662070L;

    static IntervalType mul(String value0, IntervalType value1) {
        BigDecimal decimal = new BigDecimal(CastWithString.longCastWithStringCompat(value0.split("\\.")[0]));
        return buildInterval(decimal, value1);
    }

    static IntervalType mul(Integer value0, IntervalType value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return buildInterval(decimal, value1);
    }

    static IntervalType mul(Long value0, IntervalType value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return buildInterval(decimal, value1);
    }

    static IntervalType mul(Double value0, IntervalType value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return buildInterval(decimal, value1);
    }

    static IntervalType mul(Float value0, IntervalType value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return buildInterval(decimal, value1);
    }

    static IntervalType mul(Boolean value0, IntervalType value1) {
        BigDecimal decimal = new BigDecimal(value0 ? 1 : 0);
        return buildInterval(decimal, value1);
    }

    static IntervalType mul(BigDecimal value0, IntervalType value1) {
        BigDecimal decimal = new BigDecimal(Math.round(CastWithString.doubleCastWithStringCompat(value0.toString())));
        return buildInterval(decimal, value1);
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
}
