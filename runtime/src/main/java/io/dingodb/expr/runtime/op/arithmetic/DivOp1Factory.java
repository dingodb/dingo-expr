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

package io.dingodb.expr.runtime.op.arithmetic;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;


public class DivOp1Factory extends DivOp {

    public static final DivOp1Factory INSTANCE = new DivOp1Factory();
    @Serial
    private static final long serialVersionUID = -8087135512942367227L;

    private final Map<Object, DivOp> opMap = new HashMap<>();

    private DivOp1Factory() {
        super();
        opMap.put(keyOf(Types.FLOAT, Types.FLOAT), new DivOp1Factory.DivFloatFloat());
        opMap.put(keyOf(Types.DECIMAL, Types.DECIMAL), new DivOp1Factory.DivDecimalDecimal());
        opMap.put(keyOf(Types.INT, Types.INT), new DivOp1Factory.DivIntInt());
        opMap.put(keyOf(Types.LONG, Types.LONG), new DivOp1Factory.DivLongLong());
        opMap.put(keyOf(Types.DOUBLE, Types.DOUBLE), new DivOp1Factory.DivDoubleDouble());
    }

    @Override
    public BinaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class DivFloatFloat extends DivOp {

        @Serial
        private static final long serialVersionUID = 3149920684645912128L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                         ExprConfig config) {
            return div((Float) value0, (Float) value1);
        }

        @Override
        public Type getType() {
            return Types.FLOAT;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT, Types.FLOAT);
        }
    }

    public static final class DivDecimalDecimal extends DivOp {

        @Serial
        private static final long serialVersionUID = 4481902942997870570L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                              ExprConfig config) {
            if (value0 instanceof BigDecimal && value1 instanceof BigDecimal) {
                BigDecimal value0Decimal = (BigDecimal) value0;
                BigDecimal value1Decimal = (BigDecimal) value1;
                int leftScale = value0Decimal.scale();

                if (value1Decimal.compareTo(BigDecimal.ZERO) != 0) {
                    return value0Decimal.divide(value1Decimal, leftScale, RoundingMode.HALF_UP);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }

        @Override
        public Type getType() {
            return Types.DECIMAL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL, Types.DECIMAL);
        }
    }

    public static final class DivIntInt extends DivOp {

        @Serial
        private static final long serialVersionUID = -1044298367948649528L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                          ExprConfig config) {
            return div((Integer) value0, (Integer) value1);
        }

        @Override
        public Type getType() {
            return Types.DOUBLE;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT, Types.INT);
        }
    }

    public static final class DivLongLong extends DivOp {


        @Serial
        private static final long serialVersionUID = 6592631565770909715L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                          ExprConfig config) {
            return div((Long) value0, (Long) value1);
        }

        @Override
        public Type getType() {
            return Types.DOUBLE;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG, Types.LONG);
        }
    }

    public static final class DivDoubleDouble extends DivOp {


        @Serial
        private static final long serialVersionUID = -2939341971102323838L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                          ExprConfig config) {
            return div((Double) value0, (Double) value1);
        }

        @Override
        public Type getType() {
            return Types.DOUBLE;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE, Types.DOUBLE);
        }
    }

}
