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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public final class StringCastOpFactory extends StringCastOp {

    public static final StringCastOpFactory INSTANCE = new StringCastOpFactory();
    @Serial
    private static final long serialVersionUID = -911258631217335192L;

    private final Map<Object, StringCastOp> opMap = new HashMap<>();

    private static final StringCastAny stringCastAny = new StringCastAny();

    private StringCastOpFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new StringCastFloat());
        opMap.put(keyOf(Types.BYTES), new StringCastBytes());
        opMap.put(keyOf(Types.NULL), new StringCastNull());
        opMap.put(keyOf(Types.DECIMAL), new StringCastDecimal());
        opMap.put(keyOf(Types.BOOL), new StringCastBool());
        opMap.put(keyOf(Types.LONG), new StringCastLong());
        opMap.put(keyOf(Types.TIME), new StringCastTime());
        opMap.put(keyOf(Types.STRING), new StringCastString());
        opMap.put(keyOf(Types.DOUBLE), new StringCastDouble());
        opMap.put(keyOf(Types.TIMESTAMP), new StringCastTimestamp());
        opMap.put(keyOf(Types.INT), new StringCastInt());
        opMap.put(keyOf(Types.DATE), new StringCastDate());
        opMap.put(keyOf(Types.ANY), new StringCastAny());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class StringCastFloat extends StringCastOp {
        private static final long serialVersionUID = 2387559150584906758L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class StringCastBytes extends StringCastOp {
        private static final long serialVersionUID = -8779470692888063507L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BYTES);
        }
    }

    public static final class StringCastNull extends StringCastOp {
        private static final long serialVersionUID = 4826739661679772522L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class StringCastDecimal extends StringCastOp {
        private static final long serialVersionUID = 1412013538212723207L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class StringCastBool extends StringCastOp {
        private static final long serialVersionUID = 4213288685433569327L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class StringCastLong extends StringCastOp {
        private static final long serialVersionUID = 2134610802432118049L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class StringCastTime extends StringCastOp {
        private static final long serialVersionUID = -8682115430711214779L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.TIME);
        }
    }

    public static final class StringCastString extends StringCastOp {
        private static final long serialVersionUID = -4900171577771769298L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class StringCastDouble extends StringCastOp {
        private static final long serialVersionUID = -1045901546652901261L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class StringCastTimestamp extends StringCastOp {
        private static final long serialVersionUID = -3065090155955121700L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.TIMESTAMP);
        }
    }

    public static final class StringCastInt extends StringCastOp {
        private static final long serialVersionUID = 3798589989061206194L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class StringCastDate extends StringCastOp {
        private static final long serialVersionUID = -7775396803679655174L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return stringCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DATE);
        }
    }

    public static final class StringCastAny extends StringCastOp {

        @Serial
        private static final long serialVersionUID = 8864749213052492391L;

        @Override
        protected String evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Float) {
                return stringCast((Float) value);
            } else if (value instanceof byte[]) {
                return stringCast((byte[]) value);
            } else if (value instanceof BigDecimal) {
                return stringCast((BigDecimal) value);
            } else if (value instanceof Boolean) {
                return stringCast((Boolean) value);
            } else if (value instanceof Long) {
                return stringCast((Long) value);
            } else if (value instanceof Time) {
                return stringCast((Time) value, config);
            } else if (value instanceof Timestamp) {
                return stringCast((Timestamp) value, config);
            } else if (value instanceof Date) {
                return stringCast((Date) value, config);
            } else if (value instanceof Integer) {
                return stringCast((Integer) value);
            } else if (value instanceof Double) {
                return stringCast((Double) value);
            } else if (value instanceof String) {
                return stringCast((String) value);
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
