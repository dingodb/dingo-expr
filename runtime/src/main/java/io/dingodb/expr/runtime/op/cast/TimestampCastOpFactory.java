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
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public final class TimestampCastOpFactory extends TimestampCastOp {
    private static final long serialVersionUID = -8123219266588204483L;

    public static final TimestampCastOpFactory INSTANCE = new TimestampCastOpFactory();

    private final Map<Object, TimestampCastOp> opMap = new HashMap<>();

    private static final TimestampCastAny timestampCastAny = new TimestampCastAny();

    private TimestampCastOpFactory() {
        super();
        opMap.put(keyOf(Types.NULL), new TimestampCastNull());
        opMap.put(keyOf(Types.DECIMAL), new TimestampCastDecimal());
        opMap.put(keyOf(Types.LONG), new TimestampCastLong());
        opMap.put(keyOf(Types.STRING), new TimestampCastString());
        opMap.put(keyOf(Types.TIMESTAMP), new TimestampCastTimestamp());
        opMap.put(keyOf(Types.INT), new TimestampCastInt());
        opMap.put(keyOf(Types.DATE), new TimestampCastDate());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class TimestampCastNull extends TimestampCastOp {
        private static final long serialVersionUID = 8127672549878844347L;

        @Override
        protected Timestamp evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timestampCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class TimestampCastDecimal extends TimestampCastOp {
        private static final long serialVersionUID = -2829739277575818176L;

        @Override
        protected Timestamp evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timestampCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class TimestampCastLong extends TimestampCastOp {
        private static final long serialVersionUID = -514552312079396462L;

        @Override
        protected Timestamp evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timestampCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class TimestampCastString extends TimestampCastOp {
        private static final long serialVersionUID = 47898417915772140L;

        @Override
        protected Timestamp evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timestampCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class TimestampCastTimestamp extends TimestampCastOp {
        private static final long serialVersionUID = 819499114879243149L;

        @Override
        protected Timestamp evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timestampCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.TIMESTAMP);
        }
    }

    public static final class TimestampCastInt extends TimestampCastOp {
        private static final long serialVersionUID = 6914626982638589965L;

        @Override
        protected Timestamp evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timestampCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class TimestampCastDate extends TimestampCastOp {
        private static final long serialVersionUID = 6942116289893890527L;

        @Override
        protected Timestamp evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return timestampCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DATE);
        }
    }

    public static final class TimestampCastAny extends TimestampCastOp {

        @Serial
        private static final long serialVersionUID = 8541903441286338213L;

        @Override
        protected Timestamp evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Date) {
                return timestampCast((Date) value);
            } else if (value instanceof Integer) {
                return timestampCast((Integer) value);
            } else if (value instanceof Timestamp) {
                return timestampCast((Timestamp) value);
            } else if (value instanceof String) {
                return timestampCast((String) value, config);
            } else if (value instanceof Long) {
                return timestampCast((Long) value);
            } else if (value instanceof BigDecimal) {
                return timestampCast((BigDecimal) value);
            } else {
                return null;
            }
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DATE);
        }
    }
}
