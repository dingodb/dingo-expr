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

package io.dingodb.expr.rel.json;

import com.fasterxml.jackson.databind.module.SimpleModule;
import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpStringBuilder;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestRelOpSerdes {
    private static Parser parser;

    @BeforeAll
    public static void setupAll() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(RelOp.class, new RelOpSerializer());
        module.addDeserializer(RelOp.class, new RelOpDeserializer());
        parser = Parser.JSON.with(module);
    }

    public static @NonNull Stream<Arguments> getParameters() throws ExprParseException {
        return Stream.of(
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .filter("$[2] > 50")
                    .build(),
                "{\n"
                + "  \"filter\" : \"$[2] > 50\"\n"
                + "}"
            ),
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .project("$[0]", "$[1] * $[2]")
                    .build(),
                "{\n"
                + "  \"projects\" : [ \"$[0]\", \"$[1]*$[2]\" ]\n"
                + "}"
            ),
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .filter("$[2] > 50")
                    .project("$[0]", "$[1] * $[2]")
                    .build(),
                "{\n"
                + "  \"i\" : {\n"
                + "    \"filter\" : \"$[2] > 50\"\n"
                + "  },\n"
                + "  \"o\" : {\n"
                + "    \"projects\" : [ \"$[0]\", \"$[1]*$[2]\" ]\n"
                + "  }\n"
                + "}"
            ),
            arguments(
                RelOpStringBuilder.builder(RelConfig.DEFAULT)
                    .agg(new int[]{1, 2}, "COUNT()", "SUM($[1])")
                    .build(),
                "{\n"
                + "  \"groups\" : [ 1, 2 ],\n"
                + "  \"aggList\" : [ \"COUNT()\", \"SUM($[1])\" ]\n"
                + "}"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testSerialize(RelOp op, String json) throws IOException {
        String result = parser.stringify(op);
        assertThat(result).isEqualTo(json);
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testDeserialize(RelOp op, String json) throws IOException {
        RelOp relOp = parser.parse(
            json,
            RelOp.class,
            Collections.singletonMap(RelOpSerdesAttributes.REL_CONFIG, RelConfig.DEFAULT)
        );
        assertThat(relOp).isEqualTo(op);
    }
}
