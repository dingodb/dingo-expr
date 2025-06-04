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
import java.util.HashMap;
import java.util.Map;

public final class BytesCastOpFactory extends BytesCastOp {
    private static final long serialVersionUID = 2181111740301432814L;

    public static final BytesCastOpFactory INSTANCE = new BytesCastOpFactory();

    private final Map<Object, BytesCastOp> opMap = new HashMap<>();

    public static final BytesCastAny bytesCastAny = new BytesCastAny();

    private BytesCastOpFactory() {
        super();
        opMap.put(keyOf(Types.BYTES), new BytesCastBytes());
        opMap.put(keyOf(Types.NULL), new BytesCastNull());
        opMap.put(keyOf(Types.STRING), new BytesCastString());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class BytesCastBytes extends BytesCastOp {
        private static final long serialVersionUID = -6090107161546989877L;

        @Override
        protected byte[] evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return bytesCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BYTES);
        }
    }

    public static final class BytesCastNull extends BytesCastOp {
        private static final long serialVersionUID = -8827237454820354140L;

        @Override
        protected byte[] evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return bytesCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class BytesCastString extends BytesCastOp {
        private static final long serialVersionUID = 2939803716423179431L;

        @Override
        protected byte[] evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return bytesCastAny.evalNonNullValue(value, config);
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class BytesCastAny extends BytesCastOp {

        @Serial
        private static final long serialVersionUID = 3531758069083941720L;

        @Override
        protected byte[] evalNonNullValue(@NonNull Object value, ExprConfig config) {
            if (value instanceof String) {
                return bytesCast((String) value);
            } else if (value instanceof byte[]) {
                return bytesCast((byte[]) value);
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
