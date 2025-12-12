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
import io.dingodb.expr.common.utils.CastWithString;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.ExprContext;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static io.dingodb.expr.common.utils.CastWithString.decimalCastWithStringCompat;

public class DecimalCastOpFactory extends DecimalCastOp {

    private static final long serialVersionUID = -5114883244820122992L;

    public static final DecimalCastOpFactory INSTANCE = new DecimalCastOpFactory();

    private final Map<Object, DecimalCastOp> opMap = new HashMap<>();
    private static final DecimalCastAny decimalCastAny = new DecimalCastAny();

    private DecimalCastOpFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new DecimalCastFloat());
        opMap.put(keyOf(Types.BYTES), new DecimalCastBytes());
        opMap.put(keyOf(Types.NULL), new DecimalCastNull());
        opMap.put(keyOf(Types.DECIMAL), new DecimalCastDecimal());
        opMap.put(keyOf(Types.BOOL), new DecimalCastBool());
        opMap.put(keyOf(Types.LONG), new DecimalCastLong());
        opMap.put(keyOf(Types.STRING), new DecimalCastString());
        opMap.put(keyOf(Types.DOUBLE), new DecimalCastDouble());
        opMap.put(keyOf(Types.INT), new DecimalCastInt());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class DecimalCastFloat extends DecimalCastOp {
        private static final long serialVersionUID = -71178896811856342L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class DecimalCastBytes extends DecimalCastOp {
        private static final long serialVersionUID = -1879097110246086736L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BYTES);
        }
    }

    public static final class DecimalCastNull extends DecimalCastOp {
        private static final long serialVersionUID = -1415690319810549219L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class DecimalCastDecimal extends DecimalCastOp {
        private static final long serialVersionUID = 3012071422391580776L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class DecimalCastBool extends DecimalCastOp {
        private static final long serialVersionUID = -8835048494933669970L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class DecimalCastLong extends DecimalCastOp {
        private static final long serialVersionUID = -6578281173323175920L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class DecimalCastString extends DecimalCastOp {
        private static final long serialVersionUID = 3244045157662382980L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class DecimalCastDouble extends DecimalCastOp {
        private static final long serialVersionUID = -9045940916220657113L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class DecimalCastInt extends DecimalCastOp {
        private static final long serialVersionUID = 5119289449100656779L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return decimalCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class DecimalCastAny extends DecimalCastOp {

        @Serial
        private static final long serialVersionUID = 4003400676770665072L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Integer) {
                return decimalCast((Integer) value);
            } else if (value instanceof Long) {
                return decimalCast((Long) value);
            } else if (value instanceof BigDecimal) {
                return decimalCast((BigDecimal) value);
            } else if (value instanceof Float) {
                return decimalCast((Float) value);
            } else if (value instanceof Double) {
                return decimalCast((Double) value);
            } else if (value instanceof String) {
                if (config.getExprContext() != ExprContext.CALC_VALUE) {
                    return decimalCastWithStringCompat((String)value);
                }
                return decimalCast((String) value);
            } else if (value instanceof Boolean) {
                return decimalCast((Boolean) value);
            } else if (value instanceof byte[]) {
                return decimalCast((byte[]) value);
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
