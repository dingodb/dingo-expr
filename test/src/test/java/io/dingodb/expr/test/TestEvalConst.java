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

package io.dingodb.expr.test;

import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.expr.test.asserts.Assert;
import io.dingodb.expr.test.cases.EvalConstProvider;
import io.dingodb.expr.test.cases.EvalExceptionProvider;
import io.dingodb.expr.test.cases.WeiredEvalConstProvider;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.stream.Stream;

import static io.dingodb.expr.runtime.expr.Exprs.CURRENT_DATE;
import static io.dingodb.expr.runtime.expr.Exprs.CURRENT_TIME;
import static io.dingodb.expr.runtime.expr.Exprs.CURRENT_TIMESTAMP;
import static io.dingodb.expr.runtime.expr.Exprs.DATE_FORMAT1;
import static io.dingodb.expr.runtime.expr.Exprs.DATE_FORMAT2;
import static io.dingodb.expr.runtime.expr.Exprs.TIMESTAMP_FORMAT1;
import static io.dingodb.expr.runtime.expr.Exprs.TIMESTAMP_FORMAT2;
import static io.dingodb.expr.runtime.expr.Exprs.TIME_FORMAT1;
import static io.dingodb.expr.runtime.expr.Exprs.TIME_FORMAT2;
import static io.dingodb.expr.runtime.expr.Exprs.op;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestEvalConst {
    private static @NonNull Stream<Arguments> getParametersJavaFormat() {
        return Stream.of(
            arguments(op(DATE_FORMAT1, 0), "1970-01-01"),
            arguments(op(DATE_FORMAT2, 0, "uuuu:MM:dd"), "1970:01:01"),
            arguments(op(TIME_FORMAT1, 0), "00:00:00"),
            arguments(op(TIME_FORMAT2, 0, "HH-mm-ss"), "00-00-00"),
            arguments(op(TIMESTAMP_FORMAT1, 0), DateTimeUtils.timestampFormat(new Timestamp(0))),
            arguments(
                op(TIMESTAMP_FORMAT2, 0, "uuuuMMddHHmmss"),
                DateTimeUtils.timestampFormat(new Timestamp(0), "uuuuMMddHHmmss")
            )
        );
    }

    private static @NonNull Stream<Arguments> getParametersSqlFormat() {
        return Stream.of(
            arguments(op(DATE_FORMAT1, 0), "1970-01-01"),
            arguments(op(DATE_FORMAT2, 0, "%Y:%m:%d"), "1970:01:01"),
            arguments(op(TIME_FORMAT1, 0), "00:00:00"),
            arguments(op(TIME_FORMAT2, 0, "%H-%i-%s"), "00-00-00"),
            arguments(op(TIMESTAMP_FORMAT1, 0), DateTimeUtils.timestampFormat(new Timestamp(0))),
            arguments(
                op(TIMESTAMP_FORMAT2, 0, "%Y%m%d%H%i%s"),
                DateTimeUtils.timestampFormat(new Timestamp(0), "uuuuMMddHHmmss")
            )
        );
    }

    @ParameterizedTest
    @ArgumentsSource(EvalConstProvider.class)
    @ArgumentsSource(WeiredEvalConstProvider.class)
    @MethodSource({"getParametersJavaFormat"})
    public void testSimpleCompiler(@NonNull Expr expr, Object expected) {
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        Object v = expr1.eval();
        Assert.value(v).isEqualTo(expected);
    }

    @ParameterizedTest
    @ArgumentsSource(EvalConstProvider.class)
    @ArgumentsSource(WeiredEvalConstProvider.class)
    @MethodSource({"getParametersSqlFormat"})
    public void testForSqlCompiler(@NonNull Expr expr, Object expected) {
        Expr expr1 = ExprCompiler.FOR_SQL.visit(expr);
        assertThat(expr1).isInstanceOf(Val.class);
        Object v = expr1.eval();
        Assert.value(v).isEqualTo(expected);
    }

    @ParameterizedTest
    @ArgumentsSource(EvalExceptionProvider.class)
    public void testRangeCheck(@NonNull Expr expr, Class<? extends Exception> exceptionClass) {
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        assertThrows(exceptionClass, expr1::eval);
    }

    @Test
    public void testCurrentDate() {
        Expr expr = op(CURRENT_DATE);
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        Object v = expr1.eval();
        assertThat((Date) v).isCloseTo(DateTimeUtils.currentDate(TimeZone.getDefault()), 5000);
    }

    @Test
    public void testCurrentTime() {
        Expr expr = op(CURRENT_TIME);
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        Object v = expr1.eval();
        assertThat((Time) v).isCloseTo(DateTimeUtils.currentTime(TimeZone.getDefault()), 5000);
    }

    @Test
    public void testCurrentTimestamp() {
        Expr expr = op(CURRENT_TIMESTAMP);
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        Object v = expr1.eval();
        assertThat((Timestamp) v).isCloseTo(DateTimeUtils.currentTimestamp(), 5000);
    }
}
