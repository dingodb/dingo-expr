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

public class TestToDebugString {
    @RegisterExtension
    static final JsonContext context = new JsonContext(
        "/simple_vars.yml"
    );

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("1 + 2", "AddOpFactory[ADD](Val[1, INT], Val[2, INT])"),
            arguments("1 + 2*3", "AddOpFactory[ADD](Val[1, INT], MulOpFactory[MUL](Val[2, INT], Val[3, INT]))"),
            arguments("1*(2 + 3)", "MulOpFactory[MUL](Val[1, INT], AddOpFactory[ADD](Val[2, INT], Val[3, INT]))")
        );
    }

    private static @NonNull Stream<Arguments> getParameters1() {
        return Stream.of(
            arguments("1 + 2", "AddIntInt[ADD](Val[1, INT], Val[2, INT])"),
            arguments("1 + 2*3", "AddIntInt[ADD](Val[1, INT], MulIntInt[MUL](Val[2, INT], Val[3, INT]))"),
            arguments("1*(2 + 3)", "MulIntInt[MUL](Val[1, INT], AddIntInt[ADD](Val[2, INT], Val[3, INT]))")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testNotCompiled(String exprString, String result) throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Assertions.assertThat(expr.toDebugString()).isEqualTo(result);
    }

    @ParameterizedTest
    @MethodSource("getParameters1")
    public void testCompiledSimple(String exprString, String result) throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr, context.getDataSchema());
        Assertions.assertThat(expr1.toDebugString()).isEqualTo(result);
    }
}
