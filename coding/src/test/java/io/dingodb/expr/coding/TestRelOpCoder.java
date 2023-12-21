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

import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpStringBuilder;
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.CodecUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestRelOpCoder {
    private static @NonNull Stream<Arguments> getParameters() throws ExprParseException {
        return Stream.of(
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .filter("$[2] > 50")
                    .build(),
                Types.tuple("INT", "STRING", "FLOAT"),
                "7134021442480000930400"
            ),
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .project("$[0]", "$[1]", "$[2] / 10")
                    .build(),
                Types.tuple("INT", "STRING", "FLOAT"),
                "726F0331000037010034021441200000860400"
            ),
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .filter("$[2] > 50")
                    .project("$[0]", "$[1]", "$[2] / 10")
                    .build(),
                Types.tuple("INT", "STRING", "FLOAT"),
                "7134021442480000930400726F0331000037010034021441200000860400"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(@NonNull RelOp op, TupleType type, String code) {
        op.init(type, RelConfig.DEFAULT);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        RelOpCoder.INSTANCE.visit(op, os);
        assertThat(os.toByteArray()).isEqualTo(CodecUtils.hexStringToBytes(code));
    }
}
