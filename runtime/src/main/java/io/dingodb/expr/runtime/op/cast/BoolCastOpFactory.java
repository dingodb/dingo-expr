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

public final class BoolCastOpFactory extends BoolCastOp {
    private static final long serialVersionUID = 2217717217827410983L;

    public static final BoolCastOpFactory INSTANCE = new BoolCastOpFactory();

    private final Map<Object, BoolCastOp> opMap = new HashMap<>();

    private static final BoolCastAny boolCastAny = new BoolCastAny();

    private BoolCastOpFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new BoolCastFloat());
        opMap.put(keyOf(Types.BYTES), new BoolCastBytes());
        opMap.put(keyOf(Types.NULL), new BoolCastNull());
        opMap.put(keyOf(Types.DECIMAL), new BoolCastDecimal());
        opMap.put(keyOf(Types.BOOL), new BoolCastBool());
        opMap.put(keyOf(Types.LONG), new BoolCastLong());
        opMap.put(keyOf(Types.DOUBLE), new BoolCastDouble());
        opMap.put(keyOf(Types.INT), new BoolCastInt());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class BoolCastFloat extends BoolCastOp {
        private static final long serialVersionUID = -2639604345052258158L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return boolCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class BoolCastBytes extends BoolCastOp {
        private static final long serialVersionUID = 2114781388161061840L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return boolCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BYTES);
        }
    }

    public static final class BoolCastNull extends BoolCastOp {
        private static final long serialVersionUID = -398254640097302226L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return boolCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class BoolCastDecimal extends BoolCastOp {
        private static final long serialVersionUID = 7261241941174830602L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return boolCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class BoolCastBool extends BoolCastOp {
        private static final long serialVersionUID = 5448422492244764652L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return boolCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class BoolCastLong extends BoolCastOp {
        private static final long serialVersionUID = 3919051389338214789L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return boolCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class BoolCastDouble extends BoolCastOp {
        private static final long serialVersionUID = -5310481950806034825L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return boolCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class BoolCastInt extends BoolCastOp {
        private static final long serialVersionUID = -369335699802975263L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return boolCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class BoolCastAny extends BoolCastOp {

        @Serial
        private static final long serialVersionUID = -5988605914876411746L;

        @Override
        protected Boolean evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof Integer) {
                return boolCast((Integer) value);
            } else if (value instanceof Long) {
                return boolCast((Long) value);
            } else if (value instanceof Double) {
                return boolCast((Double) value);
            } else if (value instanceof Float) {
                return boolCast((Float) value);
            } else if (value instanceof Boolean) {
                return boolCast((Boolean) value);
            } else if (value instanceof BigDecimal) {
                return boolCast((BigDecimal) value);
            } else if (value instanceof byte[]) {
                return boolCast((byte[]) value);
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
