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

import io.dingodb.expr.parser.ExprParser;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.expr.test.asserts.Assert;
import io.dingodb.expr.test.cases.ParseEvalConstProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestParseEvalConst {
    @ParameterizedTest
    @ArgumentsSource(ParseEvalConstProvider.class)
    public void test(String exprString, Object value) throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Expr expr1 = ExprCompiler.ADVANCED.visit(expr);
        Object result = expr1.eval();
        Assert.value(result).isEqualTo(value);
    }

    @Test
    public void testCaseFun() throws ExprParseException {
        String exprString = "case(false, 1, true, 2, 3)";
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        assertThat(expr1.getType()).isEqualTo(Types.INT);
        Object result = expr1.eval();
        Assert.value(result).isEqualTo(2);
        Expr expr2 = ExprCompiler.ADVANCED.visit(expr);
        assertThat(expr2).isEqualTo(Exprs.val(2, Types.INT));
    }

    @Test
    public void testNotFun() throws ExprParseException {
        String exprString = "! 1";
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        assertThat(expr1.getType()).isEqualTo(Types.BOOL);
        Object result = expr1.eval();
        Assert.value(result).isEqualTo(false);
        Expr expr2 = ExprCompiler.ADVANCED.visit(expr);
        assertThat(expr2).isEqualTo(Exprs.val(false, Types.BOOL));
    }

    @Test
    public void testCurrentDate() throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse("current_date()");
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        Object v = expr1.eval();
        assertThat((Date) v).isCloseTo(DateTimeUtils.currentDate(TimeZone.getDefault()), 5000);
    }

    @Test
    public void testCurrentTime() throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse("current_time()");
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        Object v = expr1.eval();
        assertThat((Time) v).isCloseTo(DateTimeUtils.currentTime(TimeZone.getDefault()), 5000);
    }

    @Test
    public void testCurrentTimestamp() throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse("current_timestamp()");
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        Object v = expr1.eval();
        assertThat((Timestamp) v).isCloseTo(DateTimeUtils.currentTimestamp(), 5000);
    }
}
