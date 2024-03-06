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

package io.dingodb.expr.rel;

import com.google.common.collect.ImmutableList;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.rel.op.RelOpStringBuilder;
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestRelOp {
    private static final String[] DATA = {
        "1, Alice, 10",
        "2, Betty, 20",
        "3, Cindy, 30",
        "4, Doris, 40",
        "5, Emily, 50",
        "6, Alice, 60",
        "7, Betty, 70",
        "8, Alice, 80",
        "9, Cindy, 90",
    };

    private static final TupleType TYPE = Types.tuple("INT", "STRING", "FLOAT");

    private static Object @NonNull [][] valueOf(TupleType type, String... csvLines) {
        SourceOp op = RelOpBuilder.builder().values(csvLines).build();
        op = (SourceOp) op.compile(new TupleCompileContextImpl(type), RelConfig.DEFAULT);
        return op.get().toArray(Object[][]::new);
    }

    private static @NonNull Stream<Arguments> getParameters() throws ExprParseException {
        return Stream.of(
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .values(DATA)
                    .filter("$[2] > 50")
                    .project("$[0]", "$[1]", "$[2] / 10")
                    .build(),
                TYPE,
                valueOf(
                    TYPE,
                    "6, Alice, 6.0",
                    "7, Betty, 7.0",
                    "8, Alice, 8.0",
                    "9, Cindy, 9.0"
                )
            ),
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .values(DATA)
                    .agg("COUNT()")
                    .build(),
                Types.tuple("INT", "STRING", "FLOAT"),
                new Object[][]{
                    new Object[]{9L},
                }
            ),
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .values(DATA)
                    .agg(new int[]{1}, "COUNT()", "SUM($[2])", "MIN($[2])", "MAX($[2])")
                    .build(),
                Types.tuple("INT", "STRING", "FLOAT"),
                valueOf(
                    Types.tuple("STRING", "LONG", "FLOAT", "FLOAT", "FLOAT"),
                    "Alice, 3, 150, 10, 80",
                    "Betty, 2, 90, 20, 70",
                    "Cindy, 2, 120, 30, 90",
                    "Doris, 1, 40, 40, 40",
                    "Emily, 1, 50, 50, 50"
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(@NonNull SourceOp op, TupleType type, @NonNull Object[][] result) {
        op = (SourceOp) op.compile(new TupleCompileContextImpl(type), RelConfig.DEFAULT);
        assertThat(op.get()).containsExactlyInAnyOrder(result);
    }

    @Test
    public void testSingleValue() throws ExprParseException {
        RelOp op = RelOpStringBuilder.builder(RelConfig.DEFAULT)
            .values("100")
            .agg("SINGLE_VALUE_AGG($[0])")
            .build();
        SourceOp sourceOp = (SourceOp) op.compile(
            new TupleCompileContextImpl(Types.tuple(Types.INT)),
            RelConfig.DEFAULT
        );
        assertThat(sourceOp.get()).containsExactlyElementsOf(ImmutableList.of(new Object[]{100}));
    }
}
