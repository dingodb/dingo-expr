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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class AddOpFactory1 extends AddOp {
    private static final long serialVersionUID = 8796866975921256156L;

    public static final AddOpFactory1 INSTANCE = new AddOpFactory1();
    public static final AddAnyAny addAnyAny = new AddAnyAny();

    private final Map<Object, AddOp> opMap = new HashMap<>();

    private AddOpFactory1() {
        super();
        opMap.put(keyOf(Types.FLOAT, Types.FLOAT), new AddFloatFloat());
        opMap.put(keyOf(Types.DECIMAL, Types.DECIMAL), new AddDecimalDecimal());
        opMap.put(keyOf(Types.INT, Types.INT), new AddIntInt());
        opMap.put(keyOf(Types.LONG, Types.LONG), new AddLongLong());
        opMap.put(keyOf(Types.STRING, Types.STRING), new AddStringString());
        opMap.put(keyOf(Types.DOUBLE, Types.DOUBLE), new AddDoubleDouble());
        opMap.put(Types.ANY, new AddAnyAny());
    }

    @Override
    public BinaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class AddFloatFloat extends AddOp {
        private static final long serialVersionUID = -561870987851657575L;

        @Override
        protected Float evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                         ExprConfig config) {
            Object val = addAnyAny.evalNonNullValue(value0, value1, config);
            if (val instanceof Float) {
                return (Float) val;
            } else {
                return new BigDecimal(val.toString()).floatValue();
            }
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

    public static final class AddDecimalDecimal extends AddOp {
        private static final long serialVersionUID = -648960025041463924L;

        @Override
        protected BigDecimal evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                              ExprConfig config) {
            Object val = addAnyAny.evalNonNullValue(value0, value1, config);
            if (val instanceof BigDecimal) {
                return (BigDecimal) val;
            } else {
                return new BigDecimal(val.toString());
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

    public static final class AddIntInt extends AddOp {
        private static final long serialVersionUID = 1455452432674438487L;

        @Override
        protected Integer evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                           ExprConfig config) {
            Object val = addAnyAny.evalNonNullValue(value0, value1, config);
            if (val instanceof Integer) {
                return (Integer) val;
            } else {
                return new BigDecimal(val.toString()).intValue();
            }
        }

        @Override
        public Type getType() {
            return Types.INT;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT, Types.INT);
        }
    }

    public static final class AddLongLong extends AddOp {
        private static final long serialVersionUID = 4095519633405050615L;

        @Override
        protected Long evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                        ExprConfig config) {
            Object val = addAnyAny.evalNonNullValue(value0, value1, config);
            if (val instanceof Long) {
                return (Long) val;
            } else {
                return new BigDecimal(val.toString()).longValue();
            }
        }

        @Override
        public Type getType() {
            return Types.LONG;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG, Types.LONG);
        }
    }

    public static final class AddStringString extends AddOp {
        private static final long serialVersionUID = -5375143994270239804L;

        @Override
        protected String evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                          ExprConfig config) {
            return addAnyAny.evalNonNullValue(value0, value1, config).toString();
        }

        @Override
        public Type getType() {
            return Types.STRING;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING, Types.STRING);
        }
    }

    public static final class AddDoubleDouble extends AddOp {
        private static final long serialVersionUID = 2066473014842430626L;

        @Override
        protected Double evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                          ExprConfig config) {
            Object val = addAnyAny.evalNonNullValue(value0, value1, config);
            if (val instanceof Double) {
                return (Double) val;
            } else {
                return new BigDecimal(val.toString()).doubleValue();
            }
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

    public static final class AddAnyAny extends AddOp {
        private static final long serialVersionUID = 2066473014842430626L;

        @Override
        protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                          ExprConfig config) {
            if (value0 instanceof Integer) {
                if (value1 instanceof Integer) {
                    return (Integer)value0 + (Integer)value1;
                } else if (value1 instanceof Long) {
                    return ((Integer) value0).longValue() + (long) value1;
                } else if (value1 instanceof Double) {
                    return ((Integer) value0).doubleValue() + (double) value1;
                } else if (value1 instanceof Float) {
                    return ((Integer) value0).floatValue() + (float) value1;
                } else if (value1 instanceof Number) {
                    return new BigDecimal(value0.toString()).add(new BigDecimal(value1.toString()));
                } else {
                    try {
                        BigDecimal v1 = new BigDecimal(value1.toString());
                        return (Integer)value0 + v1.intValue();
                    } catch (Exception e) {
                        return value0;
                    }
                }
            } else if (value0 instanceof Long) {
                if (value1 instanceof Long) {
                    return (Long)value0 + ((Long) value1);
                } else if (value1 instanceof Integer) {
                    return (long)value0 + ((Integer) value1).longValue();
                } else if (value1 instanceof Double) {
                    return ((Long) value0).doubleValue() + (double) value1;
                } else if (value1 instanceof Float) {
                    return ((Long) value0).floatValue() + (float) value1;
                } else if (value1 instanceof Number) {
                    return new BigDecimal(value0.toString()).add(new BigDecimal(value1.toString()));
                } else {
                    try {
                        BigDecimal v1 = new BigDecimal(value1.toString());
                        return (Long)value0 + v1.longValue();
                    } catch (Exception e) {
                        return value0;
                    }
                }
            } else if (value0 instanceof Float) {
                if (value1 instanceof Float) {
                    return (Float)value0 + ((Float) value1);
                } else if (value1 instanceof Long) {
                    return (float)value0 + ((Long) value1).floatValue();
                } else if (value1 instanceof Integer) {
                    return (float)value0 + ((Integer) value1).floatValue();
                } else if (value1 instanceof Double) {
                    return ((Float) value0).doubleValue() + (double) value1;
                } else if (value1 instanceof Number) {
                    return new BigDecimal(value0.toString()).add(new BigDecimal(value1.toString()));
                } else {
                    try {
                        BigDecimal v1 = new BigDecimal(value1.toString());
                        return (Float)value0 + v1.floatValue();
                    } catch (Exception e) {
                        return value0;
                    }
                }
            } else if (value0 instanceof Double) {
                if (value1 instanceof Double) {
                    return (Double)value0 + ((Double) value1);
                } else if (value1 instanceof Float) {
                    return (double) value0 + ((Float) value1).doubleValue();
                } else if (value1 instanceof Long) {
                    return (double) value0 + ((Long) value1).doubleValue();
                } else if (value1 instanceof Integer) {
                    return (double) value0 + ((Integer) value1).doubleValue();
                } else if (value1 instanceof Number) {
                    return new BigDecimal(value0.toString()).add(new BigDecimal(value1.toString()));
                } else {
                    try {
                        BigDecimal v1 = new BigDecimal(value1.toString());
                        return (Double)value0 + v1.doubleValue();
                    } catch (Exception e) {
                        return value0;
                    }
                }
            } else if (value0 instanceof BigDecimal) {
                if (value1 instanceof Number) {
                    return ((BigDecimal)value0).add((new BigDecimal(value1.toString())));
                } else {
                    try {
                        BigDecimal v1 = new BigDecimal(value1.toString());
                        return ((BigDecimal)value0).add(v1);
                    } catch (Exception e) {
                        return value0;
                    }
                }
            } else if (value0 instanceof String) {
                BigDecimal v0;
                try {
                    v0 = new BigDecimal(value0.toString());
                } catch (Exception e) {
                    v0 = new BigDecimal(0);
                }
                if (value1 instanceof Number) {
                    return v0.add((new BigDecimal(value1.toString()))).toString();
                } else {
                    try {
                        BigDecimal v1 = new BigDecimal(value1.toString());
                        return v0.add(v1).toString();
                    } catch (Exception e) {
                        return v0.toString();
                    }
                }
            } else {
                return "0";
            }
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
