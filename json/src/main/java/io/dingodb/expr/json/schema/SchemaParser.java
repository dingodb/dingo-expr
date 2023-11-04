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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.expr.json.runtime.DataFormat;
import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.expr.json.runtime.SchemaRoot;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;

public final class SchemaParser extends Parser {
    public static final SchemaParser JSON = new SchemaParser(DataFormat.APPLICATION_JSON);
    public static final SchemaParser YAML = new SchemaParser(DataFormat.APPLICATION_YAML);

    private static final long serialVersionUID = -6361417870291149731L;

    private SchemaParser(DataFormat format) {
        super(format);
    }

    public static SchemaParser get(@NonNull DataFormat format) {
        switch (format) {
            case APPLICATION_JSON:
                return JSON;
            case APPLICATION_YAML:
                return YAML;
            default:
                throw new IllegalArgumentException("Unsupported format \"" + format + "\".");
        }
    }

    /**
     * Parse an input string into a {@link SchemaRoot}.
     *
     * @param json the input string
     * @return the {@link SchemaRoot}
     * @throws JsonProcessingException if something is wrong
     */
    public @NonNull SchemaRoot parse(String json) throws JsonProcessingException {
        Schema schema = parse(json, Schema.class);
        return new SchemaRoot(schema.createDataSchema());
    }

    /**
     * Read a given {@link InputStream} and parse the contents into a {@link SchemaRoot}.
     *
     * @param is the input string
     * @return the {@link SchemaRoot}
     * @throws IOException if something is wrong
     */
    public @NonNull SchemaRoot parse(InputStream is) throws IOException {
        Schema schema = parse(is, Schema.class);
        return new SchemaRoot(schema.createDataSchema());
    }

    /**
     * Serialize (the {@link io.dingodb.expr.json.runtime.DataSchema} of) a {@link SchemaRoot} into a string.
     *
     * @param schemaRoot the {@link SchemaRoot}
     * @return the string
     * @throws JsonProcessingException if something is wrong
     */
    public String serialize(@NonNull SchemaRoot schemaRoot) throws JsonProcessingException {
        return stringify(schemaRoot.getSchema());
    }
}
