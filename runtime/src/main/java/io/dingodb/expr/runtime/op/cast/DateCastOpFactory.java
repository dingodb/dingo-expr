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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public final class DateCastOpFactory extends DateCastOp {
    private static final long serialVersionUID = -7287646274654425353L;

    public static final DateCastOpFactory INSTANCE = new DateCastOpFactory();

    private final Map<Object, DateCastOp> opMap = new HashMap<>();

    private static final DateCastAny dateCastAny = new DateCastAny();

    private DateCastOpFactory() {
        super();
        opMap.put(keyOf(Types.NULL), new DateCastNull());
        opMap.put(keyOf(Types.LONG), new DateCastLong());
        opMap.put(keyOf(Types.STRING), new DateCastString());
        opMap.put(keyOf(Types.TIMESTAMP), new DateCastTimestamp());
        opMap.put(keyOf(Types.INT), new DateCastInt());
        opMap.put(keyOf(Types.DATE), new DateCastDate());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class DateCastNull extends DateCastOp {
        private static final long serialVersionUID = -2710875258834133114L;

        @Override
        protected Date evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return dateCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class DateCastLong extends DateCastOp {
        private static final long serialVersionUID = -2470849819436998465L;

        @Override
        protected Date evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return dateCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class DateCastString extends DateCastOp {
        private static final long serialVersionUID = 2068562594096919895L;

        @Override
        protected Date evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return dateCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class DateCastTimestamp extends DateCastOp {
        private static final long serialVersionUID = -6468461040475745273L;

        @Override
        protected Date evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return dateCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.TIMESTAMP);
        }
    }

    public static final class DateCastInt extends DateCastOp {
        private static final long serialVersionUID = -5617961640450970188L;

        @Override
        protected Date evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return dateCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class DateCastDate extends DateCastOp {
        private static final long serialVersionUID = 4034801585542271336L;

        @Override
        protected Date evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return dateCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DATE);
        }
    }

    public static final class DateCastAny extends DateCastOp {

        @Serial
        private static final long serialVersionUID = -8980056971101697871L;

        @Override
        protected Date evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Date) {
                return dateCast((Date) value);
            } else if (value instanceof Integer) {
                return dateCast((Integer) value);
            } else if (value instanceof Timestamp) {
                return dateCast((Timestamp) value);
            } else if (value instanceof String) {
                return dateCast((String) value, config);
            } else if (value instanceof Long) {
                return dateCast((Long) value);
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
