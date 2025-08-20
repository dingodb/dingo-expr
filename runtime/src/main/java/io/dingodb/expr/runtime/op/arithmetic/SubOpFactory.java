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
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubOpFactory extends SubOp {
    private static final long serialVersionUID = 6784963294675340840L;
    public static final SubOpFactory INSTANCE = new SubOpFactory();
    private final Map<Object, SubOp> opMap = new HashMap();
    private static final SubAnyAny subAnyAny = new SubAnyAny();

    private SubOpFactory() {
        this.opMap.put(this.keyOf(Types.FLOAT, Types.FLOAT), new SubFloatFloat());
        this.opMap.put(this.keyOf(Types.DECIMAL, Types.DECIMAL), new SubDecimalDecimal());
        this.opMap.put(this.keyOf(Types.INT, Types.INT), new SubIntInt());
        this.opMap.put(this.keyOf(Types.LONG, Types.LONG), new SubLongLong());
        this.opMap.put(this.keyOf(Types.DOUBLE, Types.DOUBLE), new SubDoubleDouble());
    }

    public BinaryOp getOp(OpKey key) {
        return (BinaryOp)this.opMap.get(key);
    }

    public static final class SubFloatFloat extends SubOp {
        private static final long serialVersionUID = 907844066737946569L;

        public SubFloatFloat() {
        }

        protected Float evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = subAnyAny.evalNonNullValue(value0, value1, config);
            return val instanceof Float ? (Float)val : (new BigDecimal(val.toString())).floatValue();
        }

        public Type getType() {
            return Types.FLOAT;
        }

        public OpKey getKey() {
            return this.keyOf(Types.FLOAT, Types.FLOAT);
        }
    }

    public static final class SubDecimalDecimal extends SubOp {
        private static final long serialVersionUID = 456896626235990403L;

        public SubDecimalDecimal() {
        }

        protected BigDecimal evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = subAnyAny.evalNonNullValue(value0, value1, config);
            return val instanceof BigDecimal ? (BigDecimal)val : new BigDecimal(val.toString());
        }

        public Type getType() {
            return Types.DECIMAL;
        }

        public OpKey getKey() {
            return this.keyOf(Types.DECIMAL, Types.DECIMAL);
        }
    }

    public static final class SubIntInt extends SubOp {
        private static final long serialVersionUID = 5122318645409829077L;

        public SubIntInt() {
        }

        protected Integer evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = subAnyAny.evalNonNullValue(value0, value1, config);
            return val instanceof Integer ? (Integer) val : new BigDecimal(val.toString()).intValue();
        }

        public Type getType() {
            return Types.INT;
        }

        public OpKey getKey() {
            return this.keyOf(Types.INT, Types.INT);
        }
    }

    public static final class SubLongLong extends SubOp {
        private static final long serialVersionUID = -4594083197714144603L;

        public SubLongLong() {
        }

        protected Long evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = subAnyAny.evalNonNullValue(value0, value1, config);
            return val instanceof Long ? (Long) val : new BigDecimal(val.toString()).longValue();
        }

        public Type getType() {
            return Types.LONG;
        }

        public OpKey getKey() {
            return this.keyOf(Types.LONG, Types.LONG);
        }
    }

    public static final class SubDoubleDouble extends SubOp {
        private static final long serialVersionUID = 4455773545183109883L;

        public SubDoubleDouble() {
        }

        protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = subAnyAny.evalNonNullValue(value0, value1, config);
            return val instanceof Double ? (Double) val : new BigDecimal(val.toString()).doubleValue();
        }

        public Type getType() {
            return Types.DOUBLE;
        }

        public OpKey getKey() {
            return this.keyOf(Types.DOUBLE, Types.DOUBLE);
        }
    }

    public static final class SubAnyAny extends SubOp {
        private static final long serialVersionUID = 4455773545183109883L;

        public SubAnyAny() {
        }

        protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            if (value0 instanceof Integer) {
                int value0Tmp = (int) value0;
                if (value1 instanceof Integer) {
                    return sub(value0Tmp, (Integer) value1);
                } else if (value1 instanceof Long) {
                    return sub(value0Tmp, (Long) value1);
                } else if (value1 instanceof Double) {
                    return sub(value0Tmp, (Double)value1);
                } else if (value1 instanceof Float) {
                    return sub(value0Tmp, (Float)value1);
                } else if (value1 instanceof BigDecimal) {
                    return sub(new BigDecimal(value0Tmp), (BigDecimal)value1);
                }
            } else if (value0 instanceof Long) {
                long value0Tmp = (long) value0;
                if (value1 instanceof Integer) {
                    return sub(value0Tmp, (Integer) value1);
                } else if (value1 instanceof Long) {
                    return sub(value0Tmp, (Long) value1);
                } else if (value1 instanceof Double) {
                    return sub(value0Tmp, (Double)value1);
                } else if (value1 instanceof Float) {
                    return sub(value0Tmp, (Float)value1);
                } else if (value1 instanceof BigDecimal) {
                    return sub(new BigDecimal(value0Tmp), (BigDecimal)value1);
                }
            } else if (value0 instanceof Double) {
                double value0Tmp = (double) value0;
                if (value1 instanceof Integer) {
                    return sub(value0Tmp, (Integer) value1);
                } else if (value1 instanceof Long) {
                    return sub(value0Tmp, (Long) value1);
                } else if (value1 instanceof Double) {
                    return sub(value0Tmp, (Double)value1);
                } else if (value1 instanceof Float) {
                    return sub(value0Tmp, (Float)value1);
                } else if (value1 instanceof BigDecimal) {
                    return sub(new BigDecimal(value0Tmp), (BigDecimal)value1);
                }
            } else if (value0 instanceof Float) {
                float value0Tmp = (float) value0;
                if (value1 instanceof Integer) {
                    return sub(value0Tmp, (Integer) value1);
                } else if (value1 instanceof Long) {
                    return sub(value0Tmp, (Long) value1);
                } else if (value1 instanceof Double) {
                    return sub(value0Tmp, (Double)value1);
                } else if (value1 instanceof Float) {
                    return sub(value0Tmp, (Float)value1);
                } else if (value1 instanceof BigDecimal) {
                    return sub(new BigDecimal(value0Tmp), (BigDecimal)value1);
                }
            } else if (value0 instanceof BigDecimal) {
                BigDecimal value0Tmp = (BigDecimal) value0;
                try {
                    BigDecimal value1Tmp = new BigDecimal(value1.toString());
                    return sub(value0Tmp, value1Tmp);
                } catch (Exception e) {
                    return 0;
                }
            }
            try {
                BigDecimal value0Tmp = new BigDecimal(value0.toString());
                BigDecimal value1Tmp = new BigDecimal(value1.toString());
                return sub(value0Tmp, value1Tmp);
            } catch (Exception e) {
                return 0;
            }
        }

        public Type getType() {
            return Types.ANY;
        }

        public OpKey getKey() {
            return this.keyOf(Types.ANY, Types.ANY);
        }
    }
}

