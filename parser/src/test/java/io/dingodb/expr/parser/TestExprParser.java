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

package io.dingodb.expr.parser;

import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestExprParser {
    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("1", Exprs.val(1)),
            arguments("-1", Exprs.val(-1)),
            arguments(Long.toString((long) Integer.MAX_VALUE + 1L), Exprs.val((long) Integer.MAX_VALUE + 1L)),
            arguments(Long.toString((long) Integer.MIN_VALUE - 2L), Exprs.val((long) Integer.MIN_VALUE - 2L)),
            arguments("3.5", Exprs.val(new BigDecimal("3.5"), Types.DECIMAL)),
            arguments("1E3", Exprs.val(new BigDecimal("1E3"), Types.DECIMAL)),
            arguments("'123'", Exprs.val("123")),
            arguments("true", Exprs.var("true")),
            arguments("false", Exprs.var("false")),
            arguments("null", Exprs.var("null")),
            arguments("a", Exprs.var("a")),
            arguments("+a", Exprs.op(Exprs.POS, Exprs.var("a"))),
            arguments("-a", Exprs.op(Exprs.NEG, Exprs.var("a"))),
            arguments("a + b", Exprs.op(Exprs.ADD, Exprs.var("a"), Exprs.var("b"))),
            arguments("a - b", Exprs.op(Exprs.SUB, Exprs.var("a"), Exprs.var("b"))),
            arguments("a * b", Exprs.op(Exprs.MUL, Exprs.var("a"), Exprs.var("b"))),
            arguments("a / b", Exprs.op(Exprs.DIV, Exprs.var("a"), Exprs.var("b"))),
            arguments("a = b", Exprs.op(Exprs.EQ, Exprs.var("a"), Exprs.var("b"))),
            arguments("a != b", Exprs.op(Exprs.NE, Exprs.var("a"), Exprs.var("b"))),
            arguments("a <> b", Exprs.op(Exprs.NE, Exprs.var("a"), Exprs.var("b"))),
            arguments("a > b", Exprs.op(Exprs.GT, Exprs.var("a"), Exprs.var("b"))),
            arguments("a >= b", Exprs.op(Exprs.GE, Exprs.var("a"), Exprs.var("b"))),
            arguments("a < b", Exprs.op(Exprs.LT, Exprs.var("a"), Exprs.var("b"))),
            arguments("a <= b", Exprs.op(Exprs.LE, Exprs.var("a"), Exprs.var("b"))),
            arguments("a && b", Exprs.op(Exprs.AND, Exprs.var("a"), Exprs.var("b"))),
            arguments("a || b", Exprs.op(Exprs.OR, Exprs.var("a"), Exprs.var("b"))),
            arguments("!a", Exprs.op(Exprs.NOT, Exprs.var("a"))),
            arguments(
                "3 * (a + b)",
                Exprs.op(Exprs.MUL, Exprs.val(3), Exprs.op(Exprs.ADD, Exprs.var("a"), Exprs.var("b")))
            )
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, Expr result) throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        assertThat(expr).isEqualTo(result);
    }
}
