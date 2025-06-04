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

public final class DoubleCastOpFactory extends DoubleCastOp {
    private static final long serialVersionUID = -5477208717628123675L;

    public static final DoubleCastOpFactory INSTANCE = new DoubleCastOpFactory();

    private final Map<Object, DoubleCastOp> opMap = new HashMap<>();

    static final DoubleCastAny doubleCastAny = new DoubleCastAny();

    private DoubleCastOpFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new DoubleCastFloat());
        opMap.put(keyOf(Types.BYTES), new DoubleCastBytes());
        opMap.put(keyOf(Types.NULL), new DoubleCastNull());
        opMap.put(keyOf(Types.DECIMAL), new DoubleCastDecimal());
        opMap.put(keyOf(Types.BOOL), new DoubleCastBool());
        opMap.put(keyOf(Types.LONG), new DoubleCastLong());
        opMap.put(keyOf(Types.STRING), new DoubleCastString());
        opMap.put(keyOf(Types.DOUBLE), new DoubleCastDouble());
        opMap.put(keyOf(Types.INT), new DoubleCastInt());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class DoubleCastFloat extends DoubleCastOp {
        private static final long serialVersionUID = 4790585256694318670L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class DoubleCastBytes extends DoubleCastOp {
        private static final long serialVersionUID = -5280135940523424713L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BYTES);
        }
    }

    public static final class DoubleCastNull extends DoubleCastOp {
        private static final long serialVersionUID = 3054817265437942836L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class DoubleCastDecimal extends DoubleCastOp {
        private static final long serialVersionUID = 5993146757117966321L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class DoubleCastBool extends DoubleCastOp {
        private static final long serialVersionUID = 3321811212118501551L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class DoubleCastLong extends DoubleCastOp {
        private static final long serialVersionUID = 7824565184217535864L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class DoubleCastString extends DoubleCastOp {
        private static final long serialVersionUID = -1890069285457354320L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class DoubleCastDouble extends DoubleCastOp {
        private static final long serialVersionUID = 7069947978785204586L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class DoubleCastInt extends DoubleCastOp {
        private static final long serialVersionUID = -4568413383698767028L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return doubleCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class DoubleCastAny extends DoubleCastOp {

        @Serial
        private static final long serialVersionUID = -2087485151971805179L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Integer) {
                return doubleCast((Integer) value);
            } else if (value instanceof Double) {
                return doubleCast((Double)value);
            } else if (value instanceof Float) {
                return doubleCast((Float)value);
            } else if (value instanceof Long) {
                return doubleCast((Long)value);
            } else if (value instanceof String) {
                return doubleCast((String)value);
            } else if (value instanceof Boolean) {
                return doubleCast((Boolean) value);
            } else if (value instanceof BigDecimal) {
                return doubleCast((BigDecimal) value);
            } else if (value instanceof byte[]) {
                return doubleCast((byte[]) value);
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
