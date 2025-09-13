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

public final class DivOpFactory extends DivOp {
    private static final long serialVersionUID = -6469594318220686032L;
    public static final DivOpFactory INSTANCE = new DivOpFactory();
    private final Map<Object, DivOp> opMap = new HashMap();
    private static final DivAnyAny divAnyAny = new DivAnyAny();

    private DivOpFactory() {
        this.opMap.put(this.keyOf(Types.FLOAT, Types.FLOAT), new DivFloatFloat());
        this.opMap.put(this.keyOf(Types.DECIMAL, Types.DECIMAL), new DivDecimalDecimal());
        this.opMap.put(this.keyOf(Types.INT, Types.INT), new DivIntInt());
        this.opMap.put(this.keyOf(Types.LONG, Types.LONG), new DivLongLong());
        this.opMap.put(this.keyOf(Types.DOUBLE, Types.DOUBLE), new DivDoubleDouble());
    }

    public BinaryOp getOp(OpKey key) {
        return (BinaryOp)this.opMap.get(key);
    }

    public static final class DivFloatFloat extends DivOp {
        private static final long serialVersionUID = 4754785335734613362L;

        public DivFloatFloat() {
        }

        protected Float evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = divAnyAny.evalNonNullValue(value0, value1, config);
            if (val == null) {
                return null;
            }
            return val instanceof Float ? (Float)val : (new BigDecimal(val.toString())).floatValue();
        }

        public Type getType() {
            return Types.FLOAT;
        }

        public OpKey getKey() {
            return this.keyOf(Types.FLOAT, Types.FLOAT);
        }
    }

    public static final class DivDecimalDecimal extends DivOp {
        private static final long serialVersionUID = -1353437752763383432L;

        public DivDecimalDecimal() {
        }

        protected BigDecimal evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = divAnyAny.evalNonNullValue(value0, value1, config);
            if (val == null) {
                return null;
            }
            return val instanceof BigDecimal ? (BigDecimal)val : new BigDecimal(val.toString());
        }

        public Type getType() {
            return Types.DECIMAL;
        }

        public OpKey getKey() {
            return this.keyOf(Types.DECIMAL, Types.DECIMAL);
        }
    }

    public static final class DivIntInt extends DivOp {
        private static final long serialVersionUID = 1969065905399287590L;

        public DivIntInt() {
        }

        protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = divAnyAny.evalNonNullValue(value0, value1, config);
            if (val == null) {
                return null;
            }
            return val instanceof Double ? (Double)val : (new BigDecimal(val.toString())).doubleValue();
        }

        public Type getType() {
            return Types.DOUBLE;
        }

        public OpKey getKey() {
            return this.keyOf(Types.INT, Types.INT);
        }
    }

    public static final class DivLongLong extends DivOp {
        private static final long serialVersionUID = -1062167998258980495L;

        public DivLongLong() {
        }

        protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = divAnyAny.evalNonNullValue(value0, value1, config);
            if (val == null) {
                return null;
            }
            return val instanceof Double ? (Double)val : (new BigDecimal(val.toString())).doubleValue();
        }

        public Type getType() {
            return Types.DOUBLE;
        }

        public OpKey getKey() {
            return this.keyOf(Types.LONG, Types.LONG);
        }
    }

    public static final class DivDoubleDouble extends DivOp {
        private static final long serialVersionUID = -4438406023339517679L;

        public DivDoubleDouble() {
        }

        protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            Object val = divAnyAny.evalNonNullValue(value0, value1, config);
            if (val == null) {
                return null;
            }
            return val instanceof Double ? (Double)val : (new BigDecimal(val.toString())).doubleValue();
        }

        public Type getType() {
            return Types.DOUBLE;
        }

        public OpKey getKey() {
            return this.keyOf(Types.DOUBLE, Types.DOUBLE);
        }
    }

    public static final class DivAnyAny extends DivOp {
        private static final long serialVersionUID = -4438406023339517679L;

        public DivAnyAny() {
        }

        protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            if (value0 instanceof Integer) {
                int value0Tmp = (int) value0;
                if (value1 instanceof Integer) {
                    return div(value0Tmp, (Integer) value1);
                } else if (value1 instanceof Long) {
                    return div(value0Tmp, (Long) value1);
                } else if (value1 instanceof Double) {
                    return div(value0Tmp, (Double)value1);
                } else if (value1 instanceof Float) {
                    return div(value0Tmp, (Float)value1);
                } else if (value1 instanceof BigDecimal) {
                    return div(new BigDecimal(value0Tmp), (BigDecimal)value1);
                }
            } else if (value0 instanceof Long) {
                long value0Tmp = (long) value0;
                if (value1 instanceof Integer) {
                    return div(value0Tmp, (Integer) value1);
                } else if (value1 instanceof Long) {
                    return div(value0Tmp, (Long) value1);
                } else if (value1 instanceof Double) {
                    return div(value0Tmp, (Double)value1);
                } else if (value1 instanceof Float) {
                    return div(value0Tmp, (Float)value1);
                } else if (value1 instanceof BigDecimal) {
                    return div(new BigDecimal(value0Tmp), (BigDecimal)value1);
                }
            } else if (value0 instanceof Double) {
                double value0Tmp = (double) value0;
                if (value1 instanceof Integer) {
                    return div(value0Tmp, (Integer) value1);
                } else if (value1 instanceof Long) {
                    return div(value0Tmp, (Long) value1);
                } else if (value1 instanceof Double) {
                    return div(value0Tmp, (Double)value1);
                } else if (value1 instanceof Float) {
                    return div(value0Tmp, (Float)value1);
                } else if (value1 instanceof BigDecimal) {
                    return div(new BigDecimal(value0Tmp), (BigDecimal)value1);
                }
            } else if (value0 instanceof Float) {
                float value0Tmp = (float) value0;
                if (value1 instanceof Integer) {
                    return div(value0Tmp, (Integer) value1);
                } else if (value1 instanceof Long) {
                    return div(value0Tmp, (Long) value1);
                } else if (value1 instanceof Double) {
                    return div(value0Tmp, (Double)value1);
                } else if (value1 instanceof Float) {
                    return div(value0Tmp, (Float)value1);
                } else if (value1 instanceof BigDecimal) {
                    return div(new BigDecimal(value0Tmp), (BigDecimal)value1);
                }
            } else if (value0 instanceof BigDecimal) {
                BigDecimal value0Tmp = (BigDecimal) value0;
                try {
                    BigDecimal value1Tmp = new BigDecimal(value1.toString());
                    return div(value0Tmp, value1Tmp);
                } catch (Exception e) {
                    return 0;
                }
            }
            try {
                BigDecimal value0Tmp = new BigDecimal(value0.toString());
                BigDecimal value1Tmp = new BigDecimal(value1.toString());
                return div(value0Tmp, value1Tmp);
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
