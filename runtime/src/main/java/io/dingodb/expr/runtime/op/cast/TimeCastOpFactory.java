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

package io.dingodb.expr.runtime.op.cast;

import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public final class TimeCastOpFactory extends TimeCastOp {
    private static final long serialVersionUID = 8428404453006648035L;

    public static final TimeCastOpFactory INSTANCE = new TimeCastOpFactory();

    private final Map<Object, TimeCastOp> opMap = new HashMap<>();

    private static final TimeCastAny timeCastAny = new TimeCastAny();

    private TimeCastOpFactory() {
        super();
        opMap.put(keyOf(Types.NULL), new TimeCastNull());
        opMap.put(keyOf(Types.LONG), new TimeCastLong());
        opMap.put(keyOf(Types.TIME), new TimeCastTime());
        opMap.put(keyOf(Types.STRING), new TimeCastString());
        opMap.put(keyOf(Types.INT), new TimeCastInt());
        opMap.put(keyOf(Types.DATE), new TimeCastAny());
        opMap.put(keyOf(Types.TIMESTAMP), new TimeCastAny());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class TimeCastNull extends TimeCastOp {
        private static final long serialVersionUID = -1167078016743632605L;

        @Override
        protected Time evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timeCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class TimeCastLong extends TimeCastOp {
        private static final long serialVersionUID = -5571019848193748198L;

        @Override
        protected Time evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timeCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class TimeCastTime extends TimeCastOp {
        private static final long serialVersionUID = -2075775727034681768L;

        @Override
        protected Time evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timeCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.TIME);
        }
    }

    public static final class TimeCastString extends TimeCastOp {
        private static final long serialVersionUID = -6155928174455503826L;

        @Override
        protected Time evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timeCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class TimeCastInt extends TimeCastOp {
        private static final long serialVersionUID = 8758591464479537566L;

        @Override
        protected Time evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timeCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class TimeCastAny extends TimeCastOp {

        @Serial
        private static final long serialVersionUID = -3471725027131368701L;

        @Override
        protected Time evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Integer) {
                return timeCast((Integer) value);
            } else if (value instanceof String) {
                return timeCast((String) value, config);
            } else if (value instanceof Time) {
                return timeCast((Time) value);
            } else if (value instanceof Long) {
                return timeCast((Long) value);
            } else if (value instanceof Timestamp) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(((Timestamp) value).getTime());

                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                int minute = calendar.get(Calendar.MINUTE);
                int second = calendar.get(Calendar.SECOND);

                return new Time(hour, minute, second);
            } else {
                return null;
            }
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.ANY);
        }
    }
}
