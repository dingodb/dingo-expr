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

import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.TandemOp;
import io.dingodb.expr.rel.dto.FilterOpDto;
import io.dingodb.expr.rel.dto.GroupedAggregateOpDto;
import io.dingodb.expr.rel.dto.ProjectOpDto;
import io.dingodb.expr.rel.dto.RelDto;
import io.dingodb.expr.rel.dto.TandemOpDto;
import io.dingodb.expr.rel.dto.UngroupedAggregateOpDto;
import io.dingodb.expr.rel.op.FilterOp;
import io.dingodb.expr.rel.op.GroupedAggregateOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.UngroupedAggregateOp;
import org.mapstruct.Context;
import org.mapstruct.Mapper;
import org.mapstruct.SubclassExhaustiveStrategy;
import org.mapstruct.SubclassMapping;
import org.mapstruct.factory.Mappers;

@Mapper(
    uses = {
        ExprMapper.class,
        FilterOpMapper.class,
        ProjectOpMapper.class,
        TandemOpMapper.class,
    },
    subclassExhaustiveStrategy = SubclassExhaustiveStrategy.RUNTIME_EXCEPTION
)
public interface RelOpMapper {
    RelOpMapper MAPPER = Mappers.getMapper(RelOpMapper.class);

    @SubclassMapping(target = FilterOp.class, source = FilterOpDto.class)
    @SubclassMapping(target = ProjectOp.class, source = ProjectOpDto.class)
    @SubclassMapping(target = TandemOp.class, source = TandemOpDto.class)
    @SubclassMapping(target = GroupedAggregateOp.class, source = GroupedAggregateOpDto.class)
    @SubclassMapping(target = UngroupedAggregateOp.class, source = UngroupedAggregateOpDto.class)
    RelOp fromDto(RelDto dto, @Context RelConfig config);

    @SubclassMapping(target = FilterOpDto.class, source = FilterOp.class)
    @SubclassMapping(target = ProjectOpDto.class, source = ProjectOp.class)
    @SubclassMapping(target = TandemOpDto.class, source = TandemOp.class)
    @SubclassMapping(target = GroupedAggregateOpDto.class, source = GroupedAggregateOp.class)
    @SubclassMapping(target = UngroupedAggregateOpDto.class, source = UngroupedAggregateOp.class)
    RelDto toDto(RelOp op);
}
