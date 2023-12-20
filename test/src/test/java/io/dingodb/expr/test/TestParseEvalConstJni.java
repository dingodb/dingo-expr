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

import io.dingodb.expr.coding.CodingFlag;
import io.dingodb.expr.coding.ExprCoder;
import io.dingodb.expr.jni.LibExprJni;
import io.dingodb.expr.parser.ExprParser;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.test.asserts.Assert;
import io.dingodb.expr.test.cases.ParseEvalConstProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assumptions.assumeThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestParseEvalConstJni {
    @BeforeAll
    public static void setupAll() {
        LibExprJniUtils.setLibPath();
    }

    @ParameterizedTest
    @ArgumentsSource(ParseEvalConstProvider.class)
    public void testUsingJni(String exprString, Object value) throws ExprParseException {
        Expr expr = ExprParser.DEFAULT.parse(exprString);
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        assumeThat(ExprCoder.INSTANCE.visit(expr1, os)).isEqualTo(CodingFlag.OK);
        Object handle = LibExprJni.INSTANCE.decode(os.toByteArray());
        Assert.value(LibExprJni.INSTANCE.run(handle)).isEqualTo(value);
        LibExprJni.INSTANCE.release(handle);
    }
}
