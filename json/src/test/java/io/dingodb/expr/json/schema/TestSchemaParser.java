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

package io.dingodb.expr.json.schema;

import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.json.runtime.DataSchema;
import io.dingodb.expr.json.runtime.SchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSchemaParser {
    @Test
    public void testSimpleVars() throws Exception {
        SchemaRoot root = SchemaParser.YAML.parse(
            TestSchemaParser.class.getResourceAsStream("/simple_vars.yml")
        );
        assertThat(root.getMaxIndex()).isEqualTo(4);
        DataSchema schema = root.getSchema();
        assertThat(schema.getChild("a")).hasFieldOrPropertyWithValue("type", Types.LONG);
        assertThat(schema.getChild("b")).hasFieldOrPropertyWithValue("type", Types.DOUBLE);
        assertThat(schema.getChild("c")).hasFieldOrPropertyWithValue("type", Types.BOOL);
        assertThat(schema.getChild("d")).hasFieldOrPropertyWithValue("type", Types.STRING);
    }

    @Test
    public void testCompositeVars() throws Exception {
        SchemaRoot root = SchemaParser.YAML.parse(
            TestSchemaParser.class.getResourceAsStream("/composite_vars.yml")
        );
        assertThat(root.getMaxIndex()).isEqualTo(8);
        DataSchema schema = root.getSchema();
        assertThat(schema.getChild("arrA")).hasFieldOrPropertyWithValue("type", Types.ARRAY_LONG);
        assertThat(schema.getChild("arrB")).hasFieldOrPropertyWithValue("type", Types.ARRAY_STRING);
        assertThat(schema.getChild("arrC")).hasFieldOrPropertyWithValue("type", Types.LIST_ANY);
        assertThat(schema.getChild("arrD")).hasFieldOrPropertyWithValue("type", null)
            .satisfies(s -> assertThat(s.getChild(0)).hasFieldOrPropertyWithValue("type", Types.LONG))
            .satisfies(s -> assertThat(s.getChild(1)).hasFieldOrPropertyWithValue("type", Types.STRING));
        assertThat(schema.getChild("mapA")).hasFieldOrPropertyWithValue("type", Types.MAP_ANY_ANY);
        assertThat(schema.getChild("mapB")).hasFieldOrPropertyWithValue("type", null)
            .satisfies(s -> assertThat(s.getChild("foo")).hasFieldOrPropertyWithValue("type", Types.DOUBLE))
            .satisfies(s -> assertThat(s.getChild("bar")).hasFieldOrPropertyWithValue("type", Types.STRING));
    }
}
