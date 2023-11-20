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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.expr.json.runtime.DataDict;
import io.dingodb.expr.json.runtime.DataLeaf;
import io.dingodb.expr.json.runtime.DataSchema;
import io.dingodb.expr.runtime.type.Types;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;

@JsonTypeName("object")
@JsonPropertyOrder({"properties", "additionalProperties"})
public final class SchemaObject extends Schema {
    @JsonProperty("properties")
    @Getter
    private Map<String, Schema> properties;

    @JsonProperty("additionalProperties")
    @Getter
    private Boolean additionalProperties;

    @Override
    public @NonNull DataSchema createDataSchema() {
        if (additionalProperties == null || additionalProperties || properties == null) {
            return new DataLeaf(Types.MAP_ANY_ANY);
        }
        Map<String, DataSchema> children = new HashMap<>(properties.size());
        for (Map.Entry<String, Schema> entry : properties.entrySet()) {
            children.put(entry.getKey(), entry.getValue().createDataSchema());
        }
        return new DataDict(children);
    }
}
