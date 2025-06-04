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

public final class LongCastOpFactory extends LongCastOp {
    private static final long serialVersionUID = -10887221787396940L;

    public static final LongCastOpFactory INSTANCE = new LongCastOpFactory();

    private final Map<Object, LongCastOp> opMap = new HashMap<>();

    private static final LongCastAny longCastAny = new LongCastAny();

    private LongCastOpFactory() {
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

    public static final class LongCastFloat extends LongCastOp {
        private static final long serialVersionUID = 2617231607001850225L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class LongCastNull extends LongCastOp {
        private static final long serialVersionUID = 670579345153839869L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class LongCastDecimal extends LongCastOp {
        private static final long serialVersionUID = 901853789241085599L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class LongCastBool extends LongCastOp {
        private static final long serialVersionUID = -9146116717855212858L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class LongCastLong extends LongCastOp {
        private static final long serialVersionUID = -192103530067117098L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class LongCastString extends LongCastOp {
        private static final long serialVersionUID = 7390970874264156686L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class LongCastDouble extends LongCastOp {
        private static final long serialVersionUID = 2668557323923045061L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class LongCastInt extends LongCastOp {
        private static final long serialVersionUID = -3634164123437755441L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return longCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class LongCastAny extends LongCastOp {

        @Serial
        private static final long serialVersionUID = -3141079411866967357L;

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
