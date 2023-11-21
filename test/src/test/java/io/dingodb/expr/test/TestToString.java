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
import org.assertj.core.api.Assertions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestToString {
    @RegisterExtension
    static final JsonContext context = new JsonContext(
        "/simple_vars.yml"
    );

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("1 + 2", "1 + 2"),
            arguments("1 + 2*3", "1 + 2*3"),
            arguments("1*(2 + 3)", "1*(2 + 3)"),
            arguments("1 < 2 && 3 < 4 || false", "1 < 2 && 3 < 4 || false"),
            arguments("5 + a[6]*3.2 - 1.0", "5 + a[6]*3.2 - 1.0"),
            arguments("(a.b + (c + d)) * max(f, g)", "(a['b'] + c + d)*MAX(f, g)"),
            arguments("bytes('abc')", "BYTES('abc')"),
            arguments("current_date()", "CURRENT_DATE()"),
            arguments("current_time()", "CURRENT_TIME()"),
            arguments("current_timestamp()", "CURRENT_TIMESTAMP()"),
            arguments("from_unixtime(123)", "FROM_UNIXTIME(123)"),
            arguments("array(1, 2, 3)", "ARRAY(1, 2, 3)"),
            arguments("list(1, 2, 3)", "LIST(1, 2, 3)"),
            arguments("slice(array(array(1), array(2)), 0)", "SLICE(ARRAY(ARRAY(1), ARRAY(2)), 0)")
        );
    }

    private static @NonNull Stream<Arguments> getParameters1() {
        return Stream.of(
            arguments("1 + 2", "1 + 2"),
            arguments("1 + 2*3", "1 + 2*3"),
            arguments("1*(2 + 3)", "1*(2 + 3)"),
            arguments("1 < 2 && 3 < 4 || false", "1 < 2 && 3 < 4 || false"),
            arguments("5 + a*3.2 - 1.0", "DECIMAL(5) + DECIMAL($[0])*3.2 - 1.0"),
            arguments("(a + (c + b)) * max(a, b)", "(DOUBLE($[0]) + DOUBLE($[2]) + $[1])*MAX(DOUBLE($[0]), $[1])"),
            arguments("true && c", "true && $[2]"),
            arguments("c && true", "$[2] && true"),
            arguments("false && c", "false && $[2]"),
            arguments("c && false", "$[2] && false"),
            arguments("c && false && true", "$[2] && false && true"),
            arguments("bool(null) && c", "BOOL(NULL) && $[2]"),
            arguments("c && bool(null)", "$[2] && BOOL(NULL)"),
            arguments("true || c", "true || $[2]"),
            arguments("c || true", "$[2] || true"),
            arguments("false || c", "false || $[2]"),
            arguments("c || false", "$[2] || false"),
            arguments("c || false || true", "$[2] || false || true"),
            arguments("bool(null) || c", "BOOL(NULL) || $[2]"),
            arguments("c || bool(null)", "$[2] || BOOL(NULL)"),
            arguments("and(true, true, true)", "AND(true, true, true)"),
            arguments("or(false, false, false)", "OR(false, false, false)"),
            arguments("array(1, 2.0, 3)", "ARRAY(DECIMAL(1), 2.0, DECIMAL(3))"),
            arguments("list(1, 2.0, 3)", "LIST(DECIMAL(1), 2.0, DECIMAL(3))")
        );
    }

    private static @NonNull Stream<Arguments> getParameters2() {
        return Stream.of(
            arguments("1 + 2", "3"),
            arguments("1 + 2*3", "7"),
            arguments("1*(2 + 3)", "5"),
            arguments("1 < 2 && 3 < 4 or false", "true"),
            arguments("5 + a*3.2 - 1.0", "DECIMAL(5) + DECIMAL($[0])*3.2 - 1.0"),
            arguments("(a + (c + b)) * max(a, b)", "(DOUBLE($[0]) + DOUBLE($[2]) + $[1])*MAX(DOUBLE($[0]), $[1])"),
            arguments("true && c", "$[2]"),
            arguments("c && true", "$[2]"),
            arguments("false && c", "false"),
            arguments("c && false", "false"),
            arguments("c && false && true", "false"),
            arguments("bool(null) && c", "BOOL(NULL) && $[2]"),
            arguments("c && bool(null)", "$[2] && BOOL(NULL)"),
            arguments("true || c", "true"),
            arguments("c || true", "true"),
            arguments("false || c", "$[2]"),
            arguments("c || false", "$[2]"),
            arguments("c || false || true", "true"),
            arguments("bool(null) || c", "BOOL(NULL) || $[2]"),
            arguments("c || bool(null)", "$[2] || BOOL(NULL)"),
            arguments("and(true, c, null, false, null)", "false"),
            arguments("and(true, true, true)", "true"),
            arguments("and(true, null, true)", "BOOL(NULL)"),
            arguments("and(null, c, null)", "BOOL(NULL) && $[2]"),
            arguments("and(true, null, a, b, c)", "AND(BOOL(NULL), BOOL($[0]), BOOL($[1]), $[2])"),
            arguments("or(true, c, null, false, null)", "true"),
            arguments("or(false, false, false)", "false"),
            arguments("or(false, null, false)", "BOOL(NULL)"),
            arguments("or(null, c, null)", "BOOL(NULL) || $[2]"),
            arguments("or(false, null, a, b, c)", "OR(BOOL(NULL), BOOL($[0]), BOOL($[1]), $[2])"),
            arguments("bytes('abc')", "HEX('616263')")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testNotCompiled(String exprString, String result) throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Assertions.assertThat(expr.toString()).isEqualTo(result);
    }

    @ParameterizedTest
    @MethodSource("getParameters1")
    public void testCompiledSimple(String exprString, String result) throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr, context.getDataSchema());
        Assertions.assertThat(expr1.toString()).isEqualTo(result);
    }

    @ParameterizedTest
    @MethodSource("getParameters2")
    public void testCompiledAdvanced(String exprString, String result) throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Expr expr1 = ExprCompiler.FOR_SQL.visit(expr, context.getDataSchema());
        Assertions.assertThat(expr1.toString()).isEqualTo(result);
    }
}
