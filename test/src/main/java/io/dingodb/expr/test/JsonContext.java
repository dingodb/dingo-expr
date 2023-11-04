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

import io.dingodb.expr.json.runtime.DataFormat;
import io.dingodb.expr.json.runtime.DataParser;
import io.dingodb.expr.json.runtime.DataSchema;
import io.dingodb.expr.json.runtime.SchemaRoot;
import io.dingodb.expr.json.runtime.TupleEvalContext;
import io.dingodb.expr.json.schema.SchemaParser;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Optional;

public class JsonContext implements BeforeAllCallback {
    private final String schemaFileName;
    private final String[] dataStrings;

    @Getter
    private SchemaRoot schemaRoot;
    private Object[][] tuples;

    /**
     * Create a {@link JsonContext}.
     *
     * @param schemaFileName the file name of the schema
     * @param dataStrings    several datum in YAML format
     */
    public JsonContext(String schemaFileName, String... dataStrings) {
        this.schemaFileName = schemaFileName;
        this.dataStrings = dataStrings;
    }

    /**
     * Get the {@link DataSchema} corresponding the schema file.
     *
     * @return the {@link DataSchema}
     */
    public DataSchema getDataSchema() {
        return schemaRoot.getSchema();
    }

    /**
     * Get the {@link TupleEvalContext} corresponding to the data.
     *
     * @param index the index of the data
     * @return the {@link TupleEvalContext}
     */
    public TupleEvalContext getEvalContext(int index) {
        return new TupleEvalContext(tuples[index]);
    }

    @Override
    public void beforeAll(@NonNull ExtensionContext extensionContext) throws Exception {
        Optional<Class<?>> testClass = extensionContext.getTestClass();
        if (testClass.isPresent()) {
            schemaRoot = SchemaParser.get(DataFormat.fromExtension(schemaFileName))
                .parse(testClass.get().getResourceAsStream(schemaFileName));
            DataParser parser = DataParser.yaml(schemaRoot);
            tuples = new Object[dataStrings.length][];
            for (int i = 0; i < tuples.length; i++) {
                tuples[i] = parser.parse(dataStrings[i]);
            }
        }
    }
}
