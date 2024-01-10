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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.dto.RelDto;
import io.dingodb.expr.rel.mapper.RelOpMapper;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;

public class RelOpDeserializer extends StdDeserializer<RelOp> {
    private static final long serialVersionUID = 8116261001163317014L;

    protected RelOpDeserializer() {
        super(RelOp.class);
    }

    @Override
    public RelOp deserialize(
        @NonNull JsonParser jsonParser,
        @NonNull DeserializationContext deserializationContext
    ) throws IOException {
        RelDto dto = jsonParser.readValueAs(RelDto.class);
        RelConfig relConfig = (RelConfig) deserializationContext.getAttribute(RelOpSerdesAttributes.REL_CONFIG);
        return RelOpMapper.MAPPER.fromDto(dto, relConfig != null ? relConfig : RelConfig.DEFAULT);
    }
}
