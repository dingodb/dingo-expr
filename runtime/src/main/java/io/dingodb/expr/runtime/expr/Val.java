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

package io.dingodb.expr.runtime.expr;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.string.HexFunFactory;
import io.dingodb.expr.runtime.type.DateType;
import io.dingodb.expr.runtime.type.DecimalType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.LongType;
import io.dingodb.expr.runtime.type.NullType;
import io.dingodb.expr.runtime.type.TimeType;
import io.dingodb.expr.runtime.type.TimestampType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.CodecUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.text.StringEscapeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static io.dingodb.expr.runtime.utils.DateTimeUtils.toSecond;

@Getter
@EqualsAndHashCode(of = {"value", "type"})
public final class Val implements Expr {
    public static final Val NULL = new Val(null, Types.NULL);
    public static final Val NULL_INT = new Val(null, Types.INT);
    public static final Val NULL_LONG = new Val(null, Types.LONG);
    public static final Val NULL_FLOAT = new Val(null, Types.FLOAT);
    public static final Val NULL_DOUBLE = new Val(null, Types.DOUBLE);
    public static final Val NULL_BOOL = new Val(null, Types.BOOL);
    public static final Val NULL_DECIMAL = new Val(null, Types.DECIMAL);
    public static final Val NULL_STRING = new Val(null, Types.STRING);
    public static final Val NULL_BYTES = new Val(null, Types.BYTES);
    public static final Val NULL_DATE = new Val(null, Types.DATE);
    public static final Val NULL_TIME = new Val(null, Types.TIME);
    public static final Val NULL_TIMESTAMP = new Val(null, Types.TIMESTAMP);
    public static final Val NULL_ANY = new Val(null, Types.ANY);

    public static final Val TAU = new Val(6.283185307179586476925, Types.DOUBLE);
    public static final Val E = new Val(2.7182818284590452354, Types.DOUBLE);

    public static final Val TRUE = new Val(true, Types.BOOL);
    public static final Val FALSE = new Val(false, Types.BOOL);

    private static final long serialVersionUID = -5457707032677852803L;

    @Getter
    private final Object value;
    @Getter
    private final Type type;

    Val(Object value, Type type) {
        this.value = value;
        this.type = type;
    }

    public static @NonNull Val parseLiteralInt(String text) {
        try {
            return new Val(Integer.parseInt(text), Types.INT);
        } catch (NumberFormatException e1) { // overflow
            try {
                return new Val(Long.parseLong(text), Types.LONG);
            } catch (NumberFormatException e2) { // overflow
                return new Val(new BigDecimal(text), Types.DECIMAL);
            }
        }
    }

    public static @NonNull Val parseLiteralReal(String text) {
        return new Val(new BigDecimal(text), Types.DECIMAL);
    }

    public static @NonNull Val parseLiteralString(@NonNull String text) {
        return new Val(StringEscapeUtils.unescapeJson(text.substring(1, text.length() - 1)), Types.STRING);
    }

    private static @NonNull String wrapByFun(String funName, String valueStr) {
        return funName + "(" + valueStr + ")";
    }

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        return value;
    }

    @Override
    public @NonNull Expr simplify(ExprConfig config) {
        return this;
    }

    @Override
    public <R, T> R accept(@NonNull ExprVisitor<R, T> visitor, T obj) {
        return visitor.visitVal(this, obj);
    }

    @Override
    public String toString() {
        Expr expr = ExprCompiler.SIMPLE.visit(this);
        return smartToString(expr.eval());
    }

    private String smartToString(Object obj) {
        if (obj == null) {
            return type.equals(Types.NULL) ? NullType.NAME : wrapByFun(type.toString(), NullType.NAME);
        }
        if (obj instanceof String) {
            return "'" + StringEscapeUtils.escapeJson((String) obj) + "'";
        }
        if (obj instanceof Long
            && Integer.MIN_VALUE <= (Long) obj
            && (Long) obj <= Integer.MAX_VALUE
        ) {
            return wrapByFun(LongType.NAME, Long.toString((Long) obj));
        }
        if (obj instanceof BigDecimal
            && ((BigDecimal) obj).scale() == 0
            && 0 <= ((BigDecimal) obj).compareTo(BigDecimal.valueOf(Long.MIN_VALUE))
            && ((BigDecimal) obj).compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0
        ) {
            return wrapByFun(DecimalType.NAME, obj.toString());
        }
        if (obj instanceof Float) {
            return wrapByFun(FloatType.NAME, Float.toString((Float) obj));
        }
        if (obj instanceof Double) {
            return wrapByFun(DoubleType.NAME, Double.toString((Double) obj));
        }
        if (obj instanceof Date) {
            return wrapByFun(DateType.NAME, toSecond(((Date) obj).getTime(), 0).toString());
        }
        if (obj instanceof Time) {
            return wrapByFun(TimeType.NAME, toSecond(((Time) obj).getTime(), 3).toString());
        }
        if (obj instanceof Timestamp) {
            return wrapByFun(TimestampType.NAME, toSecond(((Timestamp) obj).getTime(), 3).toString());
        }
        if (obj instanceof byte[]) {
            return wrapByFun(HexFunFactory.NAME, "'" + CodecUtils.bytesToHexString((byte[]) obj) + "'");
        }
        return obj.toString();
    }
}
