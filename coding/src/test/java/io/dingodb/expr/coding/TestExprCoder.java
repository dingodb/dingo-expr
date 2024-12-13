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

package io.dingodb.expr.coding;

import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.utils.CodecUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.util.stream.Stream;

import static io.dingodb.expr.runtime.expr.Exprs.ADD;
import static io.dingodb.expr.runtime.expr.Exprs.AND;
import static io.dingodb.expr.runtime.expr.Exprs.EQ;
import static io.dingodb.expr.runtime.expr.Exprs.GT;
import static io.dingodb.expr.runtime.expr.Exprs.IS_NULL;
import static io.dingodb.expr.runtime.expr.Exprs.IS_TRUE;
import static io.dingodb.expr.runtime.expr.Exprs.LT;
import static io.dingodb.expr.runtime.expr.Exprs.MUL;
import static io.dingodb.expr.runtime.expr.Exprs.TO_LONG;
import static io.dingodb.expr.runtime.expr.Exprs.op;
import static io.dingodb.expr.runtime.expr.Exprs.val;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestExprCoder {
    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments(val(1), "1101"),
            arguments(val(-1), "2101"),
            arguments(val(150), "119601"),
            arguments(val(-150), "219601"),
            arguments(val(true), "13"),
            arguments(val(false), "23"),
            arguments(val(7.8), "15401F333333333333"),
            arguments(val(3.1415926), "15400921FB4D12D84A"),
            arguments(val(3E8), "1541B1E1A300000000"),
            arguments(val("abc"), "1703616263"),
            arguments(op(ADD, val(1), val(1)), "110111018301"),
            arguments(op(ADD, val(2), val(3)), "110211038301"),
            arguments(op(ADD, val(1L), val(1L)), "120112018302"),
            arguments(op(ADD, val(2L), val(3L)), "120212038302"),
            arguments(op(ADD, val(3), op(MUL, val(4), val(6))), "11031104110685018301"),
            arguments(op(EQ, op(ADD, val(5), val(6)), val(11)), "110511068301110B9101"),
            arguments(op(GT, val("abc"), val("a")), "17036162631701619307"),
            arguments(
                op(AND, op(GT, op(ADD, val(7), val(8)), val(14)), op(LT, val(6), val(5))),
                "110711088301110E930111061105950152"
            ),
            arguments(op(TO_LONG, val(21)), "1115F021"),
            arguments(op(AND, val(false), val(null, Types.BOOL)), "230352"),
            arguments(op(AND, val(true), val(null, Types.BOOL)), "130352"),
            arguments(op(IS_NULL, val(null, Types.INT)), "01A101"),
            arguments(op(IS_TRUE, val(1)), "1101A201")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(Expr expr, String hexString) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ExprCoder.INSTANCE.visit(ExprCompiler.SIMPLE.visit(expr), os);
        assertThat(os.toByteArray()).isEqualTo(CodecUtils.hexStringToBytes(hexString));
    }
}
