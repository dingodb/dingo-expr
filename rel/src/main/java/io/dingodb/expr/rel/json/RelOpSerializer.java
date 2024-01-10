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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.dto.RelDto;
import io.dingodb.expr.rel.mapper.RelOpMapper;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;

public class RelOpSerializer extends StdSerializer<RelOp> {
    private static final long serialVersionUID = -8583331434963797569L;

    protected RelOpSerializer() {
        super(RelOp.class);
    }

    @Override
    public void serialize(
        RelOp relOp,
        @NonNull JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider
    ) throws IOException {
        RelDto dto = RelOpMapper.MAPPER.toDto(relOp);
        jsonGenerator.getCodec().writeValue(jsonGenerator, dto);
    }
}
