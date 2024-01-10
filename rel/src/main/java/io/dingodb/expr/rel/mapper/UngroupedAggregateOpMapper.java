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
import io.dingodb.expr.rel.dto.UngroupedAggregateOpDto;
import io.dingodb.expr.rel.op.UngroupedAggregateOp;
import org.mapstruct.BeanMapping;
import org.mapstruct.Context;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

@Mapper(uses = {ExprMapper.class})
public interface UngroupedAggregateOpMapper {
    @BeanMapping(ignoreByDefault = true)
    @Mappings({
        @Mapping(target = "aggList", source = "aggList"),
    })
    UngroupedAggregateOp fromDto(UngroupedAggregateOpDto dto, @Context RelConfig config);

    @BeanMapping(ignoreByDefault = true)
    @Mappings({
        @Mapping(target = "aggList", source = "aggList"),
    })
    UngroupedAggregateOpDto toDto(UngroupedAggregateOp op);
}
