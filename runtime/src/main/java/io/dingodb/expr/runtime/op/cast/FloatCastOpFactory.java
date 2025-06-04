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
import java.util.HashMap;
import java.util.Map;

public final class FloatCastOpFactory extends FloatCastOp {
    private static final long serialVersionUID = 5390811599799032720L;

    public static final FloatCastOpFactory INSTANCE = new FloatCastOpFactory();

    private final Map<Object, FloatCastOp> opMap = new HashMap<>();

    private static final FloatCastAny floatCastAny = new FloatCastAny();

    private FloatCastOpFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new FloatCastFloat());
        opMap.put(keyOf(Types.BYTES), new FloatCastBytes());
        opMap.put(keyOf(Types.NULL), new FloatCastNull());
        opMap.put(keyOf(Types.DECIMAL), new FloatCastDecimal());
        opMap.put(keyOf(Types.BOOL), new FloatCastBool());
        opMap.put(keyOf(Types.LONG), new FloatCastLong());
        opMap.put(keyOf(Types.STRING), new FloatCastString());
        opMap.put(keyOf(Types.DOUBLE), new FloatCastDouble());
        opMap.put(keyOf(Types.INT), new FloatCastInt());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class FloatCastFloat extends FloatCastOp {
        private static final long serialVersionUID = -3620444123560259811L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class FloatCastBytes extends FloatCastOp {
        private static final long serialVersionUID = 6913425039083640375L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BYTES);
        }
    }

    public static final class FloatCastNull extends FloatCastOp {
        private static final long serialVersionUID = -534939089932045851L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class FloatCastDecimal extends FloatCastOp {
        private static final long serialVersionUID = 9149765374489946608L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class FloatCastBool extends FloatCastOp {
        private static final long serialVersionUID = -2440502164300192021L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class FloatCastLong extends FloatCastOp {
        private static final long serialVersionUID = 1445348729639082009L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class FloatCastString extends FloatCastOp {
        private static final long serialVersionUID = 6553364482659567033L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class FloatCastDouble extends FloatCastOp {
        private static final long serialVersionUID = 933359495285966787L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class FloatCastInt extends FloatCastOp {
        private static final long serialVersionUID = 8548593941318144595L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return floatCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class FloatCastAny extends FloatCastOp {

        @Serial
        private static final long serialVersionUID = 584710253473816879L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Integer) {
                return floatCast((Integer) value);
            } else if (value instanceof Float) {
                return floatCast((Float) value);
            } else if (value instanceof Double) {
                return floatCast((Double) value);
            } else if (value instanceof Long) {
                return floatCast((Long) value);
            } else if (value instanceof String) {
                return floatCast((String) value);
            } else if (value instanceof Boolean) {
                return floatCast((Boolean) value);
            } else if (value instanceof BigDecimal) {
                return floatCast((BigDecimal) value);
            } else if (value instanceof byte[]) {
                return floatCast((byte[]) value);
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
