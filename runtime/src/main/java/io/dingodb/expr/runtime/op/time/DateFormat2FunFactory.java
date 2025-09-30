package io.dingodb.expr.runtime.op.time;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public final class DateFormat2FunFactory extends DateFormat2Fun {
    private static final long serialVersionUID = -7943722511046298904L;

    public static final DateFormat2FunFactory INSTANCE = new DateFormat2FunFactory();

    private final Map<Object, DateFormat2Fun> opMap = new HashMap<>();

    private DateFormat2FunFactory() {
        super();
        opMap.put(keyOf(Types.ANY, Types.STRING), new DateFormatAnyString());
        opMap.put(keyOf(Types.DATE, Types.STRING), new DateFormatDateString());
        opMap.put(keyOf(Types.TIMESTAMP, Types.STRING), new DateFormatTimestampString());
    }

    @Override
    public BinaryOp getOp(OpKey key) {
        return opMap.get(key);
    }

    public static final class DateFormatAnyString extends DateFormat2Fun {
        private static final long serialVersionUID = -1014099593846559663L;

        @Override
        protected String evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                ExprConfig config) {
            if (value0 instanceof Number) {
                return dateFormat((Number) value0, (String) value1);
            } else if (value0 instanceof Timestamp) {
                return dateFormat((Timestamp) value0, (String) value1);
            } else if (value0 instanceof Date) {
                return dateFormat((Date) value0, (String) value1);
            } else {
                return null;
            }
        }

        @Override
        public Type getType() {
            return Types.STRING;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.ANY, Types.STRING);
        }
    }

    public static final class DateFormatDateString extends DateFormat2Fun {
        private static final long serialVersionUID = 1697599751798482051L;

        @Override
        protected String evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                ExprConfig config) {
            if (value0 instanceof Number) {
                return dateFormat((Number) value0, (String) value1);
            } else if (value0 instanceof Timestamp) {
                return dateFormat((Timestamp) value0, (String) value1);
            } else if (value0 instanceof Date) {
                return dateFormat((Date) value0, (String) value1);
            } else {
                return null;
            }
        }

        @Override
        public Type getType() {
            return Types.STRING;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.DATE, Types.STRING);
        }
    }

    public static final class DateFormatTimestampString extends DateFormat2Fun {
        private static final long serialVersionUID = 9159110028878236722L;

        @Override
        protected String evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                ExprConfig config) {
            if (value0 instanceof Number) {
                return dateFormat((Number) value0, (String) value1);
            } else if (value0 instanceof Timestamp) {
                return dateFormat((Timestamp) value0, (String) value1);
            } else if (value0 instanceof Date) {
                return dateFormat((Date) value0, (String) value1);
            } else {
                return null;
            }
        }

        @Override
        public Type getType() {
            return Types.STRING;
        }

        @Override
        public OpKey getKey() {
            return keyOf(Types.TIMESTAMP, Types.STRING);
        }
    }
}
