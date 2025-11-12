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
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.utils.CodecUtils;
import io.dingodb.expr.test.asserts.Assert;
import io.dingodb.expr.test.cases.EvalConstProvider;
import io.dingodb.expr.test.cases.EvalExceptionProvider;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestEvalConstJni {
    @BeforeAll
    public static void setupAll() {
        LibExprJniUtils.setLibPath();
    }

    /*
    @ParameterizedTest
    @ArgumentsSource(EvalConstProvider.class)
    public void testSimpleCompiler(@NonNull Expr expr, Object expected) {
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        assumeThat(ExprCoder.INSTANCE.visit(expr1, os)).isEqualTo(CodingFlag.OK);
        Object handle = LibExprJni.INSTANCE.decode(os.toByteArray());
        Assert.value(LibExprJni.INSTANCE.run(handle)).isEqualTo(expected);
        LibExprJni.INSTANCE.release(handle);
    }

    @ParameterizedTest
    @ArgumentsSource(EvalExceptionProvider.class)
    public void testRangeCheck(@NonNull Expr expr, Class<? extends Exception> ignoredExceptionClass) {
        Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        assumeThat(ExprCoder.INSTANCE.visit(expr1, os)).isEqualTo(CodingFlag.OK);
        byte[] code = os.toByteArray();
        log.info("Coded expr: {}", CodecUtils.bytesToHexString(code));
        Object handle = LibExprJni.INSTANCE.decode(code);
        Exception exception = assertThrows(RuntimeException.class, () -> LibExprJni.INSTANCE.run(handle));
        log.info(exception.getMessage());
        LibExprJni.INSTANCE.release(handle);
    }
     */
}
