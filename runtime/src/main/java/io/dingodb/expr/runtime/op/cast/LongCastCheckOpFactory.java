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

public final class LongCastCheckOpFactory extends LongCastCheckOp {
    private static final long serialVersionUID = 8340314820276961876L;

    public static final LongCastCheckOpFactory INSTANCE = new LongCastCheckOpFactory();

    private final Map<Object, LongCastCheckOp> opMap = new HashMap<>();

    private static final LongCastAny longCastAny = new LongCastAny();

    private LongCastCheckOpFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new LongCastFloat());
        opMap.put(keyOf(Types.NULL), new LongCastNull());
        opMap.put(keyOf(Types.DECIMAL), new LongCastDecimal());
        opMap.put(keyOf(Types.BOOL), new LongCastBool());
        opMap.put(keyOf(Types.LONG), new LongCastLong());
        opMap.put(keyOf(Types.STRING), new LongCastString());
        opMap.put(keyOf(Types.DOUBLE), new LongCastDouble());
        opMap.put(keyOf(Types.INT), new LongCastInt());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class LongCastFloat extends LongCastCheckOp {
        private static final long serialVersionUID = -1765893222667651337L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class LongCastNull extends LongCastCheckOp {
        private static final long serialVersionUID = -3564163640585212095L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class LongCastDecimal extends LongCastCheckOp {
        private static final long serialVersionUID = -232958099127786423L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class LongCastBool extends LongCastCheckOp {
        private static final long serialVersionUID = 3395743214832226598L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class LongCastLong extends LongCastCheckOp {
        private static final long serialVersionUID = 8523416667972014498L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class LongCastString extends LongCastCheckOp {
        private static final long serialVersionUID = 1185466683530901829L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class LongCastDouble extends LongCastCheckOp {
        private static final long serialVersionUID = -237815898384886930L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class LongCastInt extends LongCastCheckOp {
        private static final long serialVersionUID = -6134661363936393131L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class LongCastAny extends LongCastCheckOp {

        @Serial
        private static final long serialVersionUID = -107779219311764830L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Integer) {
                return longCast((Integer) value);
            } else if (value instanceof Long) {
                return longCast((Long) value);
            } else if (value instanceof Double) {
                return longCast((Double) value);
            } else if (value instanceof Float) {
                return longCast((Float) value);
            } else if (value instanceof BigDecimal) {
                return longCast((BigDecimal) value);
            } else if (value instanceof String) {
                return longCast((String) value);
            } else if (value instanceof Boolean) {
                return longCast((Boolean) value);
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
