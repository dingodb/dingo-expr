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

public final class IntCastOpFactory extends IntCastOp {
    private static final long serialVersionUID = -8638612226608431425L;

    public static final IntCastOpFactory INSTANCE = new IntCastOpFactory();

    private final Map<Object, IntCastOp> opMap = new HashMap<>();

    private static final IntCastAny intCastAny = new IntCastAny();

    private IntCastOpFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new IntCastFloat());
        opMap.put(keyOf(Types.NULL), new IntCastNull());
        opMap.put(keyOf(Types.DECIMAL), new IntCastDecimal());
        opMap.put(keyOf(Types.BOOL), new IntCastBool());
        opMap.put(keyOf(Types.LONG), new IntCastLong());
        opMap.put(keyOf(Types.STRING), new IntCastString());
        opMap.put(keyOf(Types.DOUBLE), new IntCastDouble());
        opMap.put(keyOf(Types.INT), new IntCastInt());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class IntCastFloat extends IntCastOp {
        private static final long serialVersionUID = -6065723914794866841L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class IntCastNull extends IntCastOp {
        private static final long serialVersionUID = -5233005404560786830L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class IntCastDecimal extends IntCastOp {
        private static final long serialVersionUID = 3970763523851473589L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class IntCastBool extends IntCastOp {
        private static final long serialVersionUID = 8652249688874940779L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class IntCastLong extends IntCastOp {
        private static final long serialVersionUID = 7015339667964145008L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class IntCastString extends IntCastOp {
        private static final long serialVersionUID = 1655708292954820707L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class IntCastDouble extends IntCastOp {
        private static final long serialVersionUID = -5660651412266246897L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class IntCastInt extends IntCastOp {
        private static final long serialVersionUID = 3573097796431461634L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return intCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class IntCastAny extends IntCastOp {

        @Serial
        private static final long serialVersionUID = 5945992117234812711L;

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
                return intCast((String) value);
            } else if (value instanceof BigDecimal) {
                return intCast((BigDecimal) value);
            } else if (value instanceof Boolean) {
                return intCast((Boolean) value);
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
