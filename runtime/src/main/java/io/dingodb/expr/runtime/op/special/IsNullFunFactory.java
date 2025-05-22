package io.dingodb.expr.runtime.op.special;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.UnaryOp;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public final class IsNullFunFactory extends IsNullFun {
    private static final long serialVersionUID = -3491377465619247848L;

    public static final IsNullFunFactory INSTANCE = new IsNullFunFactory();

    private final Map<Object, IsNullFun> opMap = new HashMap<>();

    private static IsNullAny isNullAny = new IsNullAny();

    private IsNullFunFactory() {
        super();
        opMap.put(keyOf(Types.FLOAT), new IsNullFloat());
        opMap.put(keyOf(Types.BYTES), new IsNullBytes());
        opMap.put(keyOf(Types.NULL), new IsNullNull());
        opMap.put(keyOf(Types.DECIMAL), new IsNullDecimal());
        opMap.put(keyOf(Types.BOOL), new IsNullBool());
        opMap.put(keyOf(Types.LONG), new IsNullLong());
        opMap.put(keyOf(Types.TIME), new IsNullTime());
        opMap.put(keyOf(Types.STRING), new IsNullString());
        opMap.put(keyOf(Types.DOUBLE), new IsNullDouble());
        opMap.put(keyOf(Types.TIMESTAMP), new IsNullTimestamp());
        opMap.put(keyOf(Types.INT), new IsNullInt());
        opMap.put(keyOf(Types.DATE), new IsNullDate());
        opMap.put(keyOf(Types.ANY), new IsNullAny());
    }

    @Override
    public UnaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class IsNullFloat extends IsNullFun {
        private static final long serialVersionUID = 6854937063895563957L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.FLOAT);
        }
    }

    public static final class IsNullBytes extends IsNullFun {
        private static final long serialVersionUID = -650773309580382143L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BYTES);
        }
    }

    public static final class IsNullNull extends IsNullFun {
        private static final long serialVersionUID = 963173293380968670L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.NULL);
        }
    }

    public static final class IsNullDecimal extends IsNullFun {
        private static final long serialVersionUID = -2908108374940966714L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DECIMAL);
        }
    }

    public static final class IsNullBool extends IsNullFun {
        private static final long serialVersionUID = -7627606605317682755L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.BOOL);
        }
    }

    public static final class IsNullLong extends IsNullFun {
        private static final long serialVersionUID = 8900549552625190978L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.LONG);
        }
    }

    public static final class IsNullTime extends IsNullFun {
        private static final long serialVersionUID = -6808258051906723971L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.TIME);
        }
    }

    public static final class IsNullString extends IsNullFun {
        private static final long serialVersionUID = -8213088305004942201L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.STRING);
        }
    }

    public static final class IsNullDouble extends IsNullFun {
        private static final long serialVersionUID = 5071315350116411970L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DOUBLE);
        }
    }

    public static final class IsNullTimestamp extends IsNullFun {
        private static final long serialVersionUID = -8387094391416254251L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.TIMESTAMP);
        }
    }

    public static final class IsNullInt extends IsNullFun {
        private static final long serialVersionUID = 6910375556982661022L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.INT);
        }
    }

    public static final class IsNullDate extends IsNullFun {
        private static final long serialVersionUID = 7164060775924691251L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return isNullAny.evalValue(value, config);
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DATE);
        }
    }

    public static final class IsNullAny extends IsNullFun {
        private static final long serialVersionUID = 7164060775924691251L;

        @Override
        public Boolean evalValue(Object value, ExprConfig config) {
            return value == null;
        }

        @Override
        public Type getType() {
            return Types.BOOL;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DATE);
        }
    }
}
