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

package io.dingodb.expr.test.cases;

import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.stream.Stream;

import static io.dingodb.expr.runtime.expr.Exprs.ABS;
import static io.dingodb.expr.runtime.expr.Exprs.ACOS;
import static io.dingodb.expr.runtime.expr.Exprs.ADD;
import static io.dingodb.expr.runtime.expr.Exprs.AND;
import static io.dingodb.expr.runtime.expr.Exprs.AND_FUN;
import static io.dingodb.expr.runtime.expr.Exprs.ASIN;
import static io.dingodb.expr.runtime.expr.Exprs.ATAN;
import static io.dingodb.expr.runtime.expr.Exprs.CEIL;
import static io.dingodb.expr.runtime.expr.Exprs.CHAR_LENGTH;
import static io.dingodb.expr.runtime.expr.Exprs.CONCAT;
import static io.dingodb.expr.runtime.expr.Exprs.COS;
import static io.dingodb.expr.runtime.expr.Exprs.COSH;
import static io.dingodb.expr.runtime.expr.Exprs.DIV;
import static io.dingodb.expr.runtime.expr.Exprs.EQ;
import static io.dingodb.expr.runtime.expr.Exprs.EXP;
import static io.dingodb.expr.runtime.expr.Exprs.FLOOR;
import static io.dingodb.expr.runtime.expr.Exprs.GE;
import static io.dingodb.expr.runtime.expr.Exprs.GT;
import static io.dingodb.expr.runtime.expr.Exprs.HEX;
import static io.dingodb.expr.runtime.expr.Exprs.IS_FALSE;
import static io.dingodb.expr.runtime.expr.Exprs.IS_NULL;
import static io.dingodb.expr.runtime.expr.Exprs.IS_TRUE;
import static io.dingodb.expr.runtime.expr.Exprs.LE;
import static io.dingodb.expr.runtime.expr.Exprs.LEFT;
import static io.dingodb.expr.runtime.expr.Exprs.LOCATE2;
import static io.dingodb.expr.runtime.expr.Exprs.LOCATE3;
import static io.dingodb.expr.runtime.expr.Exprs.LOG;
import static io.dingodb.expr.runtime.expr.Exprs.LOWER;
import static io.dingodb.expr.runtime.expr.Exprs.LT;
import static io.dingodb.expr.runtime.expr.Exprs.LTRIM;
import static io.dingodb.expr.runtime.expr.Exprs.MAX;
import static io.dingodb.expr.runtime.expr.Exprs.MID2;
import static io.dingodb.expr.runtime.expr.Exprs.MID3;
import static io.dingodb.expr.runtime.expr.Exprs.MIN;
import static io.dingodb.expr.runtime.expr.Exprs.MOD;
import static io.dingodb.expr.runtime.expr.Exprs.MUL;
import static io.dingodb.expr.runtime.expr.Exprs.NE;
import static io.dingodb.expr.runtime.expr.Exprs.NEG;
import static io.dingodb.expr.runtime.expr.Exprs.NOT;
import static io.dingodb.expr.runtime.expr.Exprs.OR;
import static io.dingodb.expr.runtime.expr.Exprs.OR_FUN;
import static io.dingodb.expr.runtime.expr.Exprs.POS;
import static io.dingodb.expr.runtime.expr.Exprs.REPEAT;
import static io.dingodb.expr.runtime.expr.Exprs.REPLACE;
import static io.dingodb.expr.runtime.expr.Exprs.REVERSE;
import static io.dingodb.expr.runtime.expr.Exprs.RIGHT;
import static io.dingodb.expr.runtime.expr.Exprs.RTRIM;
import static io.dingodb.expr.runtime.expr.Exprs.SIN;
import static io.dingodb.expr.runtime.expr.Exprs.SINH;
import static io.dingodb.expr.runtime.expr.Exprs.SUB;
import static io.dingodb.expr.runtime.expr.Exprs.SUBSTR2;
import static io.dingodb.expr.runtime.expr.Exprs.SUBSTR3;
import static io.dingodb.expr.runtime.expr.Exprs.TAN;
import static io.dingodb.expr.runtime.expr.Exprs.TANH;
import static io.dingodb.expr.runtime.expr.Exprs.TO_BOOL;
import static io.dingodb.expr.runtime.expr.Exprs.TO_BYTES;
import static io.dingodb.expr.runtime.expr.Exprs.TO_DATE;
import static io.dingodb.expr.runtime.expr.Exprs.TO_DECIMAL;
import static io.dingodb.expr.runtime.expr.Exprs.TO_DOUBLE;
import static io.dingodb.expr.runtime.expr.Exprs.TO_FLOAT;
import static io.dingodb.expr.runtime.expr.Exprs.TO_INT;
import static io.dingodb.expr.runtime.expr.Exprs.TO_INT_C;
import static io.dingodb.expr.runtime.expr.Exprs.TO_LONG;
import static io.dingodb.expr.runtime.expr.Exprs.TO_LONG_C;
import static io.dingodb.expr.runtime.expr.Exprs.TO_STRING;
import static io.dingodb.expr.runtime.expr.Exprs.TO_TIME;
import static io.dingodb.expr.runtime.expr.Exprs.TO_TIMESTAMP;
import static io.dingodb.expr.runtime.expr.Exprs.TRIM;
import static io.dingodb.expr.runtime.expr.Exprs.UPPER;
import static io.dingodb.expr.runtime.expr.Exprs._CTF;
import static io.dingodb.expr.runtime.expr.Exprs.op;
import static io.dingodb.expr.runtime.expr.Exprs.val;
import static io.dingodb.expr.runtime.expr.Val.NULL_BOOL;
import static io.dingodb.expr.runtime.expr.Val.NULL_BYTES;
import static io.dingodb.expr.runtime.expr.Val.NULL_DATE;
import static io.dingodb.expr.runtime.expr.Val.NULL_DECIMAL;
import static io.dingodb.expr.runtime.expr.Val.NULL_DOUBLE;
import static io.dingodb.expr.runtime.expr.Val.NULL_FLOAT;
import static io.dingodb.expr.runtime.expr.Val.NULL_INT;
import static io.dingodb.expr.runtime.expr.Val.NULL_LONG;
import static io.dingodb.expr.runtime.expr.Val.NULL_STRING;
import static io.dingodb.expr.runtime.expr.Val.NULL_TIME;
import static io.dingodb.expr.runtime.expr.Val.NULL_TIMESTAMP;
import static io.dingodb.expr.test.ExprsHelper.bytes;
import static io.dingodb.expr.test.ExprsHelper.date;
import static io.dingodb.expr.test.ExprsHelper.dec;
import static io.dingodb.expr.test.ExprsHelper.sec;
import static io.dingodb.expr.test.ExprsHelper.time;
import static io.dingodb.expr.test.ExprsHelper.ts;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class EvalConstProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
            arguments(val(1), 1),
            arguments(val(2L), 2L),
            arguments(val(3.3f), 3.3f),
            arguments(val(4.4), 4.4),
            arguments(val(true), true),
            arguments(val(false), false),
            arguments(dec(5.5), BigDecimal.valueOf(5.5)),
            arguments(val("abc"), "abc"),
            arguments(bytes("123"), "123".getBytes(StandardCharsets.UTF_8)),
            arguments(date(1L), new Date(sec(1L))),
            arguments(time(2L), new Time(sec(2L))),
            arguments(ts(3L), new Timestamp(sec(3L))),
            arguments(val(null), null),
            arguments(op(TO_INT, 1), 1),
            arguments(op(TO_INT, 1L), 1),
            arguments(op(TO_INT_C, 1L), 1),
            arguments(op(TO_INT, 1.4f), 1),
            arguments(op(TO_INT_C, 1.4f), 1),
            arguments(op(TO_INT, 1.5f), 2),
            arguments(op(TO_INT_C, 1.5f), 2),
            arguments(op(TO_INT, 1.4), 1),
            arguments(op(TO_INT, 1.5), 2),
            arguments(op(TO_INT, true), 1),
            arguments(op(TO_INT, false), 0),
            arguments(op(TO_INT, dec(1.4)), 1),
            arguments(op(TO_INT_C, dec(1.4)), 1),
            arguments(op(TO_INT, dec(1.5)), 2),
            arguments(op(TO_INT_C, dec(1.5)), 2),
            arguments(op(TO_INT, "10"), 10),
            arguments(op(TO_LONG, 1), 1L),
            arguments(op(TO_LONG, 1L), 1L),
            arguments(op(TO_LONG, 1.4f), 1L),
            arguments(op(TO_LONG_C, 1.4f), 1L),
            arguments(op(TO_LONG, 1.5f), 2L),
            arguments(op(TO_LONG_C, 1.4f), 1L),
            arguments(op(TO_LONG, 1.4), 1L),
            arguments(op(TO_LONG, 1.5), 2L),
            arguments(op(TO_LONG, true), 1L),
            arguments(op(TO_LONG, false), 0L),
            arguments(op(TO_LONG, dec(1.4)), 1L),
            arguments(op(TO_LONG, dec(1.5)), 2L),
            arguments(op(TO_LONG, "10"), 10L),
            arguments(op(TO_FLOAT, 1), 1.0f),
            arguments(op(TO_FLOAT, 1L), 1.0f),
            arguments(op(TO_FLOAT, 1.4f), 1.4f),
            arguments(op(TO_FLOAT, 1.4), 1.4f),
            arguments(op(TO_FLOAT, true), 1.0f),
            arguments(op(TO_FLOAT, false), 0.0f),
            arguments(op(TO_FLOAT, dec(1.4)), 1.4f),
            arguments(op(TO_FLOAT, "12.3"), 12.3f),
            arguments(op(TO_DOUBLE, 1), 1.0),
            arguments(op(TO_DOUBLE, 1L), 1.0),
            arguments(op(TO_DOUBLE, 1.4f), 1.4),
            arguments(op(TO_DOUBLE, 1.4), 1.4),
            arguments(op(TO_DOUBLE, true), 1.0),
            arguments(op(TO_DOUBLE, false), 0.0),
            arguments(op(TO_DOUBLE, dec(1.4)), 1.4),
            arguments(op(TO_DOUBLE, "12.3"), 12.3),
            arguments(op(TO_BOOL, 1), true),
            arguments(op(TO_BOOL, 0), false),
            arguments(op(TO_BOOL, 1L), true),
            arguments(op(TO_BOOL, 0L), false),
            arguments(op(TO_BOOL, 1.4f), true),
            arguments(op(TO_BOOL, 0.0f), false),
            arguments(op(TO_BOOL, 1.4), true),
            arguments(op(TO_BOOL, 0.0), false),
            arguments(op(TO_BOOL, true), true),
            arguments(op(TO_BOOL, false), false),
            arguments(op(TO_BOOL, val(BigDecimal.ONE)), true),
            arguments(op(TO_BOOL, val(BigDecimal.ZERO)), false),
            arguments(op(TO_DECIMAL, 1), BigDecimal.ONE),
            arguments(op(TO_DECIMAL, 1L), BigDecimal.ONE),
            arguments(op(TO_DECIMAL, 1.4f), BigDecimal.valueOf(1.4f)),
            arguments(op(TO_DECIMAL, 1.4), BigDecimal.valueOf(1.4)),
            arguments(op(TO_DECIMAL, true), BigDecimal.ONE),
            arguments(op(TO_DECIMAL, false), BigDecimal.ZERO),
            arguments(op(TO_DECIMAL, dec(1.4)), BigDecimal.valueOf(1.4)),
            arguments(op(TO_DECIMAL, "12.3"), BigDecimal.valueOf(12.3)),
            arguments(op(TO_STRING, 1), "1"),
            arguments(op(TO_STRING, 1L), "1"),
            arguments(op(TO_STRING, 1.4f), "1.4"),
            arguments(op(TO_STRING, 1.4), "1.4"),
            arguments(op(TO_STRING, true), "true"),
            arguments(op(TO_STRING, false), "false"),
            arguments(op(TO_STRING, dec(12.3)), "12.3"),
            arguments(op(TO_STRING, "abc"), "abc"),
            arguments(op(TO_BYTES, bytes("abc")), "abc".getBytes(StandardCharsets.UTF_8)),
            arguments(op(TO_BYTES, "abc"), "abc".getBytes(StandardCharsets.UTF_8)),
            arguments(op(TO_DATE, 1), new Date(sec(1L))),
            arguments(op(TO_DATE, 1L), new Date(sec(1L))),
            arguments(op(TO_DATE, "1970-01-01"), new Date(sec(0L))),
            arguments(op(TO_DATE, date(1L)), new Date(sec(1L))),
            arguments(op(TO_TIME, 1), new Time(sec(1L))),
            arguments(op(TO_TIME, 1L), new Time(sec(1L))),
            arguments(op(TO_TIME, "00:00:00"), new Time(sec(0L))),
            arguments(op(TO_TIME, time(1L)), new Time(sec(1L))),
            arguments(op(TO_TIMESTAMP, 1), new Timestamp(sec(1L))),
            arguments(op(TO_TIMESTAMP, 1L), new Timestamp(sec(1L))),
            arguments(
                op(TO_TIMESTAMP, "1970-01-01 00:00:00"),
                DateTimeUtils.parseTimestamp("1970-01-01 00:00:00")
            ),
            arguments(op(TO_TIMESTAMP, ts(1L)), new Timestamp(sec(1L))),
            arguments(op(POS, 1), 1),
            arguments(op(POS, 1L), 1L),
            arguments(op(POS, 1.1f), 1.1f),
            arguments(op(POS, 1.1), 1.1),
            arguments(op(POS, dec(1.1)), BigDecimal.valueOf(1.1)),
            arguments(op(NEG, 1), -1),
            arguments(op(NEG, 1L), -1L),
            arguments(op(NEG, 1.1f), -1.1f),
            arguments(op(NEG, 1.1), -1.1),
            arguments(op(NEG, dec(1.1)), BigDecimal.valueOf(-1.1)),
            arguments(op(ADD, 1, 2), 3),
            arguments(op(ADD, 1L, 2L), 3L),
            arguments(op(ADD, 1.1f, 2.2f), 3.3f),
            arguments(op(ADD, 1.1, 2.2), 3.3),
            arguments(op(ADD, dec(1.1), dec(2.2)), BigDecimal.valueOf(3.3)),
            arguments(op(ADD, 1, 2L), 3L),
            arguments(op(ADD, 1L, 2.2f), 3.2f),
            arguments(op(ADD, 1.1f, 2.2), 3.3),
            arguments(op(ADD, 1.1, dec(2.2)), BigDecimal.valueOf(3.3)),
            arguments(op(ADD, "a", "bc"), "abc"),
            arguments(op(SUB, 1, 2), -1),
            arguments(op(SUB, 1L, 2L), -1L),
            arguments(op(SUB, 1.1f, 2.2f), -1.1f),
            arguments(op(SUB, 1.1, 2.2), -1.1),
            arguments(op(SUB, dec(1.1), dec(2.2)), BigDecimal.valueOf(-1.1)),
            arguments(op(SUB, 1, 2L), -1L),
            arguments(op(SUB, 1L, 2.2f), -1.2f),
            arguments(op(SUB, 1.1f, 2.2), -1.1),
            arguments(op(SUB, 1.1, dec(2.2)), BigDecimal.valueOf(-1.1)),
            arguments(op(MUL, 1, 2), 2),
            arguments(op(MUL, 1L, 2L), 2L),
            arguments(op(MUL, 1.1f, 2.2f), 2.42f),
            arguments(op(MUL, 1.1, 2.2), 2.42),
            arguments(op(MUL, dec(1.1), dec(2.2)), BigDecimal.valueOf(2.42)),
            arguments(op(MUL, 1, 2L), 2L),
            arguments(op(MUL, 1L, 2.2f), 2.2f),
            arguments(op(MUL, 1.1f, 2.2), 2.42),
            arguments(op(MUL, 1.1, dec(2.2)), BigDecimal.valueOf(2.42)),
            arguments(op(DIV, 1, 2), 0),
            arguments(op(DIV, 1L, 2L), 0L),
            arguments(op(DIV, 1.1f, 2.2f), 0.5f),
            arguments(op(DIV, 1.1, 2.2), 0.5),
            arguments(op(DIV, dec(1.1), dec(2.2)), BigDecimal.valueOf(0.5)),
            arguments(op(DIV, 1, 2L), 0L),
            arguments(op(DIV, 1L, 2.0f), 0.5f),
            arguments(op(DIV, 1.1f, 2.2), 0.5),
            arguments(op(DIV, 1.1, dec(2.2)), BigDecimal.valueOf(0.5)),
            arguments(op(EQ, 1, 1), true),
            arguments(op(EQ, 1L, 2L), false),
            arguments(op(EQ, 1.1f, 1.1f), true),
            arguments(op(EQ, 1.1, 2.2), false),
            arguments(op(EQ, true, true), true),
            arguments(op(EQ, dec(1.1), dec(2.2)), false),
            arguments(op(EQ, "abc", "abc"), true),
            arguments(op(EQ, date(1L), date(2L)), false),
            arguments(op(EQ, time(1L), time(1L)), true),
            arguments(op(EQ, ts(2L), ts(1L)), false),
            arguments(op(NE, 1, 1), false),
            arguments(op(NE, 1L, 2L), true),
            arguments(op(NE, 1.1f, 1.1f), false),
            arguments(op(NE, 1.1, 2.2), true),
            arguments(op(NE, true, true), false),
            arguments(op(NE, dec(1.1), dec(2.2)), true),
            arguments(op(NE, "abc", "abc"), false),
            arguments(op(NE, date(1L), date(2L)), true),
            arguments(op(NE, time(1L), time(1L)), false),
            arguments(op(NE, ts(2L), ts(1L)), true),
            arguments(op(GT, 1, 1), false),
            arguments(op(GT, 1L, 2L), false),
            arguments(op(GT, 1.1f, 1.1f), false),
            arguments(op(GT, 1.1, 2.2), false),
            arguments(op(GT, true, true), false),
            arguments(op(GT, dec(1.1), dec(2.2)), false),
            arguments(op(GT, "abc", "abc"), false),
            arguments(op(GT, date(1L), date(2L)), false),
            arguments(op(GT, time(1L), time(1L)), false),
            arguments(op(GT, ts(2L), ts(1L)), true),
            arguments(op(GE, 1, 1), true),
            arguments(op(GE, 1L, 2L), false),
            arguments(op(GE, 1.1f, 1.1f), true),
            arguments(op(GE, 1.1, 2.2), false),
            arguments(op(GE, true, true), true),
            arguments(op(GE, dec(1.1), dec(2.2)), false),
            arguments(op(GE, "abc", "abc"), true),
            arguments(op(GE, date(1L), date(2L)), false),
            arguments(op(GE, time(1L), time(1L)), true),
            arguments(op(GE, ts(2L), ts(1L)), true),
            arguments(op(LT, 1, 1), false),
            arguments(op(LT, 1L, 2L), true),
            arguments(op(LT, 1.1f, 1.1f), false),
            arguments(op(LT, 1.1, 2.2), true),
            arguments(op(LT, true, true), false),
            arguments(op(LT, dec(1.1), dec(2.2)), true),
            arguments(op(LT, "abc", "abc"), false),
            arguments(op(LT, date(1L), date(2L)), true),
            arguments(op(LT, time(1L), time(1L)), false),
            arguments(op(LT, ts(2L), ts(1L)), false),
            arguments(op(LE, 1, 1), true),
            arguments(op(LE, 1L, 2L), true),
            arguments(op(LE, 1.1f, 1.1f), true),
            arguments(op(LE, 1.1, 2.2), true),
            arguments(op(LE, true, true), true),
            arguments(op(LE, dec(1.1), dec(2.2)), true),
            arguments(op(LE, "abc", "abc"), true),
            arguments(op(LE, date(1L), date(2L)), true),
            arguments(op(LE, time(1L), time(1L)), true),
            arguments(op(LE, ts(2L), ts(1L)), false),
            arguments(op(AND, false, false), false),
            arguments(op(AND, false, true), false),
            arguments(op(AND, false, NULL_BOOL), false),
            arguments(op(AND, true, false), false),
            arguments(op(AND, true, true), true),
            arguments(op(AND, true, NULL_BOOL), null),
            arguments(op(AND, NULL_BOOL, false), false),
            arguments(op(AND, NULL_BOOL, true), null),
            arguments(op(AND, NULL_BOOL, NULL_BOOL), null),
            arguments(op(OR, false, false), false),
            arguments(op(OR, false, true), true),
            arguments(op(OR, false, NULL_BOOL), null),
            arguments(op(OR, true, false), true),
            arguments(op(OR, true, true), true),
            arguments(op(OR, true, NULL_BOOL), true),
            arguments(op(OR, NULL_BOOL, false), null),
            arguments(op(OR, NULL_BOOL, true), true),
            arguments(op(OR, NULL_BOOL, NULL_BOOL), null),
            arguments(op(NOT, false), true),
            arguments(op(NOT, true), false),
            arguments(op(NOT, NULL_BOOL), null),
            arguments(op(AND_FUN, true, false, true), false),
            arguments(op(AND_FUN, true, NULL_BOOL, true), null),
            arguments(op(AND_FUN, true, true, true), true),
            arguments(op(OR_FUN, true, false, true), true),
            arguments(op(OR_FUN, false, NULL_BOOL, false), null),
            arguments(op(OR_FUN, false, false, false), false),
            arguments(op(IS_NULL, 1), false),
            arguments(op(IS_NULL, NULL_INT), true),
            arguments(op(IS_NULL, 1L), false),
            arguments(op(IS_NULL, NULL_LONG), true),
            arguments(op(IS_NULL, 1.1f), false),
            arguments(op(IS_NULL, NULL_FLOAT), true),
            arguments(op(IS_NULL, 1.1), false),
            arguments(op(IS_NULL, NULL_DOUBLE), true),
            arguments(op(IS_NULL, false), false),
            arguments(op(IS_NULL, NULL_BOOL), true),
            arguments(op(IS_NULL, dec(1)), false),
            arguments(op(IS_NULL, NULL_DECIMAL), true),
            arguments(op(IS_NULL, ""), false),
            arguments(op(IS_NULL, NULL_STRING), true),
            arguments(op(IS_NULL, bytes("")), false),
            arguments(op(IS_NULL, NULL_BYTES), true),
            arguments(op(IS_NULL, date(0)), false),
            arguments(op(IS_NULL, NULL_DATE), true),
            arguments(op(IS_NULL, time(0)), false),
            arguments(op(IS_NULL, NULL_TIME), true),
            arguments(op(IS_NULL, ts(0)), false),
            arguments(op(IS_NULL, NULL_TIMESTAMP), true),
            arguments(op(IS_TRUE, 1), true),
            arguments(op(IS_TRUE, 0), false),
            arguments(op(IS_TRUE, NULL_INT), false),
            arguments(op(IS_TRUE, 1L), true),
            arguments(op(IS_TRUE, 0L), false),
            arguments(op(IS_TRUE, NULL_LONG), false),
            arguments(op(IS_TRUE, 1.1f), true),
            arguments(op(IS_TRUE, 0.0f), false),
            arguments(op(IS_TRUE, NULL_FLOAT), false),
            arguments(op(IS_TRUE, 1.1), true),
            arguments(op(IS_TRUE, 0.0), false),
            arguments(op(IS_TRUE, NULL_DOUBLE), false),
            arguments(op(IS_TRUE, true), true),
            arguments(op(IS_TRUE, false), false),
            arguments(op(IS_TRUE, NULL_BOOL), false),
            arguments(op(IS_TRUE, dec(1)), true),
            arguments(op(IS_TRUE, dec(0)), false),
            arguments(op(IS_TRUE, NULL_DECIMAL), false),
            arguments(op(IS_TRUE, "abc"), false),
            arguments(op(IS_TRUE, ""), false),
            arguments(op(IS_TRUE, NULL_STRING), false),
            arguments(op(IS_TRUE, bytes("abc")), false),
            arguments(op(IS_TRUE, bytes("")), false),
            arguments(op(IS_TRUE, NULL_BYTES), false),
            arguments(op(IS_TRUE, date(0)), true),
            arguments(op(IS_TRUE, NULL_DATE), false),
            arguments(op(IS_TRUE, time(0)), true),
            arguments(op(IS_TRUE, NULL_TIME), false),
            arguments(op(IS_TRUE, ts(0)), true),
            arguments(op(IS_TRUE, NULL_TIMESTAMP), false),
            arguments(op(IS_FALSE, 1), false),
            arguments(op(IS_FALSE, 0), true),
            arguments(op(IS_FALSE, NULL_INT), false),
            arguments(op(IS_FALSE, 1L), false),
            arguments(op(IS_FALSE, 0L), true),
            arguments(op(IS_FALSE, NULL_LONG), false),
            arguments(op(IS_FALSE, 1.1f), false),
            arguments(op(IS_FALSE, 0.0f), true),
            arguments(op(IS_FALSE, NULL_FLOAT), false),
            arguments(op(IS_FALSE, 1.1), false),
            arguments(op(IS_FALSE, 0.0), true),
            arguments(op(IS_FALSE, NULL_DOUBLE), false),
            arguments(op(IS_FALSE, true), false),
            arguments(op(IS_FALSE, false), true),
            arguments(op(IS_FALSE, NULL_BOOL), false),
            arguments(op(IS_FALSE, dec(1)), false),
            arguments(op(IS_FALSE, dec(0)), true),
            arguments(op(IS_FALSE, NULL_DECIMAL), false),
            arguments(op(IS_FALSE, "abc"), true),
            arguments(op(IS_FALSE, ""), true),
            arguments(op(IS_FALSE, NULL_STRING), false),
            arguments(op(IS_FALSE, bytes("abc")), true),
            arguments(op(IS_FALSE, bytes("")), true),
            arguments(op(IS_FALSE, NULL_BYTES), false),
            arguments(op(IS_FALSE, date(0)), false),
            arguments(op(IS_FALSE, NULL_DATE), false),
            arguments(op(IS_FALSE, time(0)), false),
            arguments(op(IS_FALSE, NULL_TIME), false),
            arguments(op(IS_FALSE, ts(0)), false),
            arguments(op(IS_FALSE, NULL_TIMESTAMP), false),
            arguments(op(ABS, -1), 1),
            arguments(op(ABS, 1), 1),
            arguments(op(ABS, -1L), 1L),
            arguments(op(ABS, 1L), 1L),
            arguments(op(ABS, -0.5f), 0.5f),
            arguments(op(ABS, 0.5f), 0.5f),
            arguments(op(ABS, -0.5), 0.5),
            arguments(op(ABS, 0.5), 0.5),
            arguments(op(ABS, dec(-0.5)), BigDecimal.valueOf(0.5)),
            arguments(op(ABS, dec(0.5)), BigDecimal.valueOf(0.5)),
            arguments(op(MIN, 1, 2), 1),
            arguments(op(MIN, 1L, 2L), 1L),
            arguments(op(MIN, 1.1f, 2.2f), 1.1f),
            arguments(op(MIN, 1.1, 2.2), 1.1),
            arguments(op(MIN, dec(1.1), dec(2.2)), BigDecimal.valueOf(1.1)),
            arguments(op(MIN, "abc", "def"), "abc"),
            arguments(op(MIN, date(1L), date(2L)), new Date(sec(1L))),
            arguments(op(MIN, time(1L), time(2L)), new Time(sec(1L))),
            arguments(op(MIN, ts(1L), ts(2L)), new Timestamp(sec(1L))),
            arguments(op(MAX, 1, 2), 2),
            arguments(op(MAX, 1L, 2L), 2L),
            arguments(op(MAX, 1.1f, 2.2f), 2.2f),
            arguments(op(MAX, 1.1, 2.2), 2.2),
            arguments(op(MAX, dec(1.1), dec(2.2)), BigDecimal.valueOf(2.2)),
            arguments(op(MAX, "abc", "def"), "def"),
            arguments(op(MAX, date(1L), date(2L)), new Date(sec(2L))),
            arguments(op(MAX, time(1L), time(2L)), new Time(sec(2L))),
            arguments(op(MAX, ts(1L), ts(2L)), new Timestamp(sec(2L))),
            arguments(op(MOD, 4, 3), 1),
            arguments(op(MOD, -4, 3), -1),
            arguments(op(MOD, 4, -3), 1),
            arguments(op(MOD, -4, -3), -1),
            arguments(op(MOD, dec(5.0), dec(2.5)), BigDecimal.valueOf(0.0)),
            arguments(op(MOD, dec(5.1), dec(2.5)), BigDecimal.valueOf(0.1)),
            arguments(op(SIN, 0), 0.0),
            arguments(op(SIN, Math.PI / 6), 0.5),
            arguments(op(SIN, Math.PI / 2), 1.0),
            arguments(op(SIN, 5 * Math.PI / 6), 0.5),
            arguments(op(SIN, Math.PI), 0.0),
            arguments(op(SIN, 7 * Math.PI / 6), -0.5),
            arguments(op(SIN, 3 * Math.PI / 2), -1.0),
            arguments(op(SIN, 11 * Math.PI / 6), -0.5),
            arguments(op(SIN, 2 * Math.PI), 0.0),
            arguments(op(COS, 0), 1.0),
            arguments(op(COS, Math.PI / 3), 0.5),
            arguments(op(COS, Math.PI / 2), 0.0),
            arguments(op(COS, 2 * Math.PI / 3), -0.5),
            arguments(op(COS, Math.PI), -1.0),
            arguments(op(COS, 4 * Math.PI / 3), -0.5),
            arguments(op(COS, 3 * Math.PI / 2), 0.0),
            arguments(op(COS, 5 * Math.PI / 3), 0.5),
            arguments(op(COS, 2 * Math.PI), 1.0),
            arguments(op(TAN, 0), 0.0),
            arguments(op(TAN, Math.PI / 4), 1.0),
            arguments(op(TAN, 3 * Math.PI / 4), -1.0),
            arguments(op(TAN, Math.PI), 0.0),
            arguments(op(TAN, 5 * Math.PI / 4), 1.0),
            arguments(op(TAN, 7 * Math.PI / 4), -1.0),
            arguments(op(TAN, 2 * Math.PI), 0.0),
            arguments(op(ASIN, -1), -Math.PI / 2),
            arguments(op(ASIN, -0.5), -Math.PI / 6),
            arguments(op(ASIN, 0), 0.0),
            arguments(op(ASIN, 0.5), Math.PI / 6),
            arguments(op(ASIN, 1), Math.PI / 2),
            arguments(op(ACOS, -1), Math.PI),
            arguments(op(ACOS, -0.5), 2 * Math.PI / 3),
            arguments(op(ACOS, 0), Math.PI / 2),
            arguments(op(ACOS, 0.5), Math.PI / 3),
            arguments(op(ACOS, 1), 0.0),
            arguments(op(ATAN, -1), -Math.PI / 4),
            arguments(op(ATAN, 0), 0.0),
            arguments(op(ATAN, 1), Math.PI / 4),
            arguments(op(SINH, 0), 0.0),
            arguments(op(COSH, 0), 1.0),
            arguments(op(TANH, 0), 0.0),
            arguments(op(EXP, 0), 1.0),
            arguments(op(EXP, 1), Math.exp(1.0)),
            arguments(op(LOG, Math.E), 1.0),
            arguments(op(LOG, 1.0 / Math.E), -1.0),
            arguments(op(CEIL, 1), 1),
            arguments(op(CEIL, 1L), 1L),
            arguments(op(CEIL, 2.3f), 3.0),
            arguments(op(CEIL, 3.4), 4.0),
            arguments(op(CEIL, dec(1.23)), BigDecimal.valueOf(2)),
            arguments(op(FLOOR, 1), 1),
            arguments(op(FLOOR, 1L), 1L),
            arguments(op(FLOOR, 2.5f), 2.0),
            arguments(op(FLOOR, 3.6), 3.0),
            arguments(op(FLOOR, dec(1.23)), BigDecimal.valueOf(1)),
            arguments(op(CHAR_LENGTH, NULL_STRING), 0),
            arguments(op(CHAR_LENGTH, ""), 0),
            arguments(op(CHAR_LENGTH, "Alice"), 5),
            arguments(op(CONCAT, NULL_STRING, "Betty"), "Betty"),
            arguments(op(CONCAT, "Alice", NULL_STRING), "Alice"),
            arguments(op(CONCAT, "Alice", "Betty"), "AliceBetty"),
            arguments(op(LOWER, "HeLLo"), "hello"),
            arguments(op(UPPER, "HeLLo"), "HELLO"),
            arguments(op(LEFT, NULL_STRING, 1), ""),
            arguments(op(LEFT, "Alice", NULL_INT), null),
            arguments(op(LEFT, "Alice", 0), ""),
            arguments(op(LEFT, "Alice", -1), ""),
            arguments(op(LEFT, "Alice", 10), "Alice"),
            arguments(op(LEFT, "Alice", 3), "Ali"),
            arguments(op(LEFT, "Alice", 3.5), "Alic"),
            arguments(op(RIGHT, NULL_STRING, 1), ""),
            arguments(op(RIGHT, "Alice", NULL_INT), null),
            arguments(op(RIGHT, "Alice", 0), ""),
            arguments(op(RIGHT, "Alice", -1), ""),
            arguments(op(RIGHT, "Alice", 10), "Alice"),
            arguments(op(RIGHT, "Alice", 3), "ice"),
            arguments(op(RIGHT, "Alice", 3.5), "lice"),
            arguments(op(TRIM, " HeLLo  "), "HeLLo"),
            arguments(op(LTRIM, NULL_STRING), null),
            arguments(op(LTRIM, " HeLLo  "), "HeLLo  "),
            arguments(op(RTRIM, NULL_STRING), null),
            arguments(op(RTRIM, " HeLLo  "), " HeLLo"),
            arguments(op(SUBSTR2, "HeLLo", 2), "LLo"),
            arguments(op(SUBSTR2, "HeLLo", 2.5), "Lo"),
            arguments(op(SUBSTR3, "HeLLo", 1, 3), "eL"),
            arguments(op(MID2, "Alice", 2), "lice"),
            arguments(op(MID2, "Alice", 0), ""),
            arguments(op(MID2, "Alice", NULL_INT), null),
            arguments(op(MID2, "Alice", -2), "ce"),
            arguments(op(MID3, NULL_STRING, 0, 0), ""),
            arguments(op(MID3, "Alice", NULL_INT, 0), null),
            arguments(op(MID3, "Alice", 1, NULL_INT), null),
            arguments(op(MID3, "Alice", 0, 0), ""),
            arguments(op(MID3, "Alice", 1, 0), ""),
            arguments(op(MID3, "Alice", 1, 3), "Ali"),
            arguments(op(MID3, "Alice", -1, 1), "e"),
            arguments(op(MID3, "Alice", -3, 2), "ic"),
            arguments(op(REPEAT, NULL_STRING, 3), null),
            arguments(op(REPEAT, "Abc", -1), ""),
            arguments(op(REPEAT, "Abc", 3), "AbcAbcAbc"),
            arguments(op(REPEAT, "Abc", 1.7), "AbcAbc"),
            arguments(op(REVERSE, NULL_STRING), null),
            arguments(op(REVERSE, "AbCdE"), "EdCbA"),
            arguments(op(REPLACE, "HeLLo", "eL", "El"), "HElLo"),
            arguments(op(LOCATE2, NULL_STRING, "a"), null),
            arguments(op(LOCATE2, "Water", "at"), 2),
            arguments(op(LOCATE2, "Water", "am"), 0),
            arguments(op(LOCATE2, "Water", NULL_STRING), null),
            arguments(op(LOCATE2, "Water", ""), 1),
            arguments(op(LOCATE3, "Water", "", 3), 3),
            arguments(op(LOCATE3, "Banana", "a", 3), 4),
            arguments(op(LOCATE3, "Banana", "a", 7), 0),
            arguments(op(LOCATE3, "Banana", "a", 3.5), 4),
            arguments(op(_CTF, "%Y"), "uuuu"),
            arguments(op(_CTF, "%Y-%m-%d"), "uuuu'-'MM'-'dd"),
            arguments(op(_CTF, "%A%B%C"), "'ABC'"),
            arguments(op(_CTF, "Year: %Y, Month: %m"), "'Year: 'uuuu', Month: 'MM"),
            arguments(op(HEX, "414243"), "ABC".getBytes(StandardCharsets.UTF_8))
        );
    }
}
