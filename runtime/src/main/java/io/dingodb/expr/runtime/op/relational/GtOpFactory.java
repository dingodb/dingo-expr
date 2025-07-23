//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.dingodb.expr.runtime.op.relational;

import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GtOpFactory extends GtOp {
    private static final long serialVersionUID = -7176134524706605436L;
    public static final GtOpFactory INSTANCE = new GtOpFactory();
    private final Map<Object, GtOp> opMap = new HashMap();

    private GtOpFactory() {
        this.opMap.put(this.keyOf(Types.FLOAT, Types.FLOAT), new GtFloatFloat());
        this.opMap.put(this.keyOf(Types.DATE, Types.DATE), new GtDateDate());
        this.opMap.put(this.keyOf(Types.TIME, Types.TIME), new GtTimeTime());
        this.opMap.put(this.keyOf(Types.DECIMAL, Types.DECIMAL), new GtDecimalDecimal());
        this.opMap.put(this.keyOf(Types.TIMESTAMP, Types.TIMESTAMP), new GtTimestampTimestamp());
        this.opMap.put(this.keyOf(Types.INT, Types.INT), new GtIntInt());
        this.opMap.put(this.keyOf(Types.BOOL, Types.BOOL), new GtBoolBool());
        this.opMap.put(this.keyOf(Types.LONG, Types.LONG), new GtLongLong());
        this.opMap.put(this.keyOf(Types.STRING, Types.STRING), new GtStringString());
        this.opMap.put(this.keyOf(Types.DOUBLE, Types.DOUBLE), new GtDoubleDouble());
    }

    public BinaryOp getOp(OpKey key) {
        return (BinaryOp)this.opMap.get(key);
    }

    public static final class GtFloatFloat extends GtOp {
        private static final long serialVersionUID = -1073057030808606182L;

        public GtFloatFloat() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((Float)value0, (Float)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.FLOAT, Types.FLOAT);
        }
    }

    public static final class GtDateDate extends GtOp {
        private static final long serialVersionUID = -3895511980759910283L;

        public GtDateDate() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((Date)value0, (Date)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.DATE, Types.DATE);
        }
    }

    public static final class GtTimeTime extends GtOp {
        private static final long serialVersionUID = -6628043315789648268L;

        public GtTimeTime() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((Time)value0, (Time)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.TIME, Types.TIME);
        }
    }

    public static final class GtDecimalDecimal extends GtOp {
        private static final long serialVersionUID = 3105068316559288562L;

        public GtDecimalDecimal() {
        }

        private final BigDecimal toBigDecimal(@NonNull Object value) {
            //This is a ugly way to deal with the parameter types.
            //Should not be merged into main branch.
            BigDecimal result = null;

            if(value instanceof BigDecimal) {
                result = (BigDecimal)value;
            } else if(value instanceof Integer) {
                result = new BigDecimal((Integer)value);
            } else if(value instanceof Long) {
                result = new BigDecimal((Long)value);
            } else if(value instanceof Float) {
                result = new BigDecimal((Float)value);
            } else if(value instanceof Double) {
                result = new BigDecimal((Double)value);
            } else if(value instanceof String) {
                result = new BigDecimal((String)value);
            } else {
                result = (BigDecimal) value;
            }

            return result;
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt(toBigDecimal(value0), toBigDecimal(value1));
        }

        public OpKey getKey() {
            return this.keyOf(Types.DECIMAL, Types.DECIMAL);
        }
    }

    public static final class GtTimestampTimestamp extends GtOp {
        private static final long serialVersionUID = -3011485637116223915L;

        public GtTimestampTimestamp() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((Timestamp)value0, (Timestamp)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.TIMESTAMP, Types.TIMESTAMP);
        }
    }

    public static final class GtIntInt extends GtOp {
        private static final long serialVersionUID = 1891838442076596761L;

        public GtIntInt() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((Integer)value0, (Integer)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.INT, Types.INT);
        }
    }

    public static final class GtBoolBool extends GtOp {
        private static final long serialVersionUID = -3028030123769549328L;

        public GtBoolBool() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((Boolean)value0, (Boolean)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.BOOL, Types.BOOL);
        }
    }

    public static final class GtLongLong extends GtOp {
        private static final long serialVersionUID = -3780303527645169456L;

        public GtLongLong() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((Long)value0, (Long)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.LONG, Types.LONG);
        }
    }

    public static final class GtStringString extends GtOp {
        private static final long serialVersionUID = 7030952680689836345L;

        public GtStringString() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((String)value0, (String)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.STRING, Types.STRING);
        }
    }

    public static final class GtDoubleDouble extends GtOp {
        private static final long serialVersionUID = -5462883683919914205L;

        public GtDoubleDouble() {
        }

        protected Boolean evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
            return gt((Double)value0, (Double)value1);
        }

        public OpKey getKey() {
            return this.keyOf(Types.DOUBLE, Types.DOUBLE);
        }
    }
}

