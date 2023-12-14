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

package io.dingodb.expr.rel.mapper;

import io.dingodb.expr.rel.PipeOp;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.SourceOp;
import io.dingodb.expr.rel.TandemOp;
import io.dingodb.expr.rel.dto.RelDto;
import io.dingodb.expr.rel.dto.TandemOpDto;
import io.dingodb.expr.rel.op.TandemPipeOp;
import io.dingodb.expr.rel.op.TandemSourceOp;
import org.mapstruct.BeanMapping;
import org.mapstruct.Context;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

@SuppressWarnings("MethodMayBeStatic")
@Mapper
public abstract class TandemOpMapper {
    public TandemOp fromDto(TandemOpDto dto, @Context RelConfig config) {
        if (dto == null) {
            return null;
        }
        RelOp input = RelOpMapper.MAPPER.fromDto(dto.getInput(), config);
        RelOp output = RelOpMapper.MAPPER.fromDto(dto.getOutput(), config);
        if (input instanceof PipeOp && output instanceof PipeOp) {
            return new TandemPipeOp((PipeOp) input, (PipeOp) output);
        } else if (input instanceof SourceOp && output instanceof PipeOp) {
            return new TandemSourceOp((SourceOp) input, (PipeOp) output);
        }
        throw new IllegalArgumentException(
            "Illegal input/output op type \""
                + input.getClass().getCanonicalName()
                + "\" and \""
                + output.getClass().getCanonicalName()
                + "\".");
    }

    @BeanMapping(ignoreByDefault = true)
    @Mappings({
        @Mapping(target = "input", source = "input"),
        @Mapping(target = "output", source = "output"),
    })
    public abstract TandemOpDto toDto(TandemOp op);

    public RelDto relOpToRelDto(RelOp op) {
        return RelOpMapper.MAPPER.toDto(op);
    }
}
