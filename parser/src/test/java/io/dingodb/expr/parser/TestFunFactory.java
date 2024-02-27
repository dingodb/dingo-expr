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

import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFunFactory {
    @Test
    public void testCallUnregisteredFun() {
        assertThrows(ExprParseException.class,
            () -> ExprParser.DEFAULT.parse("non_exist('world')"));
    }

    @Test
    public void testRegisterFun() throws Exception {
        ExprParser.DEFAULT.getFunFactory().registerUnaryFun("hello", new HelloOp());
        Expr expr = ExprParser.DEFAULT.parse("hello('world')");
        assertThat(ExprCompiler.SIMPLE.visit(expr).eval()).isEqualTo("Hello world");
    }

    static class HelloOp extends UnaryOp {
        public static final HelloOp INSTANCE = new HelloOp();

        private static final long serialVersionUID = 9001403224696253161L;

        @Override
        public Object evalNonNullValue(@NonNull Object value, ExprConfig config) {
            return "Hello " + value;
        }

        @Override
        public Type getType() {
            return Types.STRING;
        }

        @Override
        public UnaryOp getOp(OpKey key) {
            return (key != null && key.equals(Types.STRING)) ? INSTANCE : null;
        }
    }
}
