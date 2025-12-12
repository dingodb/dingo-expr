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
import io.dingodb.expr.runtime.ExprContext;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static io.dingodb.expr.common.utils.CastWithString.intCastWithStringCompat;

public final class IntCastCheckOpFactory extends IntCastCheckOp {
    private static final long serialVersionUID = 4289317110179811760L;

    public static final IntCastCheckOpFactory INSTANCE = new IntCastCheckOpFactory();

    private final Map<Object, IntCastCheckOp> opMap = new HashMap<>();

    private static final IntCastAny intCastAny = new IntCastAny();

    private IntCastCheckOpFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new IntCastFloat());
        opMap.put(keyOf(Types.NULL), new IntCastNull());
        opMap.put(keyOf(Types.DECIMAL), new IntCastDecimal());
        opMap.put(keyOf(Types.BOOL), new IntCastBool());
        opMap.put(keyOf(Types.LONG), new IntCastLong());
        opMap.put(keyOf(Types.STRING), new IntCastString());
        opMap.put(keyOf(Types.DOUBLE), new IntCastDouble());
        opMap.put(keyOf(Types.INT), new IntCastInt());
        opMap.put(keyOf(Types.BYTES), new IntCastBytes());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class IntCastFloat extends IntCastCheckOp {
        private static final long serialVersionUID = 4728687115783377771L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class IntCastNull extends IntCastCheckOp {
        private static final long serialVersionUID = -7623238697684084739L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class IntCastDecimal extends IntCastCheckOp {
        private static final long serialVersionUID = -2986669536688919386L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class IntCastBool extends IntCastCheckOp {
        private static final long serialVersionUID = 1856680442705954193L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class IntCastLong extends IntCastCheckOp {
        private static final long serialVersionUID = 1910668940683264719L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class IntCastString extends IntCastCheckOp {
        private static final long serialVersionUID = 381446169514580081L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class IntCastDouble extends IntCastCheckOp {
        private static final long serialVersionUID = -5065861194580665564L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class IntCastInt extends IntCastCheckOp {
        private static final long serialVersionUID = -8124419310700006846L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class IntCastBytes extends IntCastCheckOp {

        @Serial
        private static final long serialVersionUID = -3226282053385258209L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class IntCastAny extends IntCastCheckOp {

        @Serial
        private static final long serialVersionUID = 8821957976579480662L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Integer) {
                return intCast((Integer) value);
            } else if (value instanceof Long) {
                return intCast((Long) value);
            } else if (value instanceof Float) {
                return intCast((Float) value);
            } else if (value instanceof Double) {
                return intCast((Double) value);
            } else if (value instanceof String) {
                if (config.getExprContext() != ExprContext.CALC_VALUE) {
                    return intCastWithStringCompat((String)value);
                }
                return intCast(Double.parseDouble((String) value));
            } else if (value instanceof BigDecimal) {
                return intCast((BigDecimal) value);
            } else if (value instanceof Boolean) {
                return intCast((Boolean) value);
            } else if (value instanceof byte[]) {
                try {
                    return Integer.parseInt(new String((byte[]) value));
                } catch (Exception e) {
                    return null;
                }
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
