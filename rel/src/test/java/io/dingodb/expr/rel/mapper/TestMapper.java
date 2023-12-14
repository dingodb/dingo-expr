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
import io.dingodb.expr.rel.RelOpBuilder;
import io.dingodb.expr.rel.TandemOp;
import io.dingodb.expr.rel.dto.FilterOpDto;
import io.dingodb.expr.rel.dto.ProjectOpDto;
import io.dingodb.expr.rel.dto.RelDto;
import io.dingodb.expr.rel.dto.TandemOpDto;
import io.dingodb.expr.rel.op.FilterOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMapper {
    @Test
    public void testFilterOpToFilterOpDto() {
        RelOp op = RelOpBuilder.builder(RelConfig.DEFAULT)
            .filter(Exprs.op(Exprs.ADD, Exprs.val(1), Exprs.var(0)))
            .build();
        RelDto dto = RelOpMapper.MAPPER.toDto(op);
        assertThat(dto)
            .isInstanceOf(FilterOpDto.class)
            .hasFieldOrPropertyWithValue("filter", "1 + $[0]");
    }

    @Test
    public void testFilterOpDtoToFilterOp() {
        FilterOpDto dto = new FilterOpDto();
        dto.setFilter("1 + $[0]");
        FilterOp op = (FilterOp) RelOpMapper.MAPPER.fromDto(dto, RelConfig.DEFAULT);
        assertThat(op.getFilter())
            .isEqualTo(Exprs.op(Exprs.ADD, Exprs.val(1), Exprs.op(Exprs.INDEX, Exprs.var("$"), Exprs.val(0))));
    }

    @Test
    public void testProjectOpToProjectOpDto() {
        RelOp op = RelOpBuilder.builder(RelConfig.DEFAULT)
            .project(Exprs.var(0), Exprs.var(1))
            .build();
        RelDto dto = RelOpMapper.MAPPER.toDto(op);
        assertThat(dto)
            .isInstanceOf(ProjectOpDto.class)
            .hasFieldOrPropertyWithValue("projects", new String[]{"$[0]", "$[1]"});
    }

    @Test
    public void testProjectOpDtoToProjectOp() {
        ProjectOpDto dto = new ProjectOpDto();
        dto.setProjects(new String[]{"$[0]", "$[1]"});
        ProjectOp op = (ProjectOp) RelOpMapper.MAPPER.fromDto(dto, RelConfig.DEFAULT);
        assertThat(op.getProjects())
            .isEqualTo(new Expr[]{
                Exprs.op(Exprs.INDEX, Exprs.var("$"), Exprs.val(0)),
                Exprs.op(Exprs.INDEX, Exprs.var("$"), Exprs.val(1))
            });
    }

    @Test
    public void testTandemOpToTandemOpDto() {
        RelOp op = RelOpBuilder.builder(RelConfig.DEFAULT)
            .filter(Exprs.op(Exprs.ADD, Exprs.val(1), Exprs.var(0)))
            .project(Exprs.var(0), Exprs.var(1))
            .build();
        RelDto dto = RelOpMapper.MAPPER.toDto(op);
        assertThat(dto).isInstanceOf(TandemOpDto.class);
        assertThat(((TandemOpDto) dto).getInput())
            .isInstanceOf(FilterOpDto.class)
            .hasFieldOrPropertyWithValue("filter", "1 + $[0]");
        assertThat(((TandemOpDto) dto).getOutput())
            .isInstanceOf(ProjectOpDto.class)
            .hasFieldOrPropertyWithValue("projects", new String[]{"$[0]", "$[1]"});
    }

    @Test
    public void testTandemOpDtoToTandemOp() {
        FilterOpDto dto0 = new FilterOpDto();
        dto0.setFilter("1 + $[0]");
        ProjectOpDto dto1 = new ProjectOpDto();
        dto1.setProjects(new String[]{"$[0]", "$[1]"});
        TandemOpDto dto = new TandemOpDto();
        dto.setInput(dto0);
        dto.setOutput(dto1);
        TandemOp op = (TandemOp) RelOpMapper.MAPPER.fromDto(dto, RelConfig.DEFAULT);
        assertThat(op.getInput())
            .isInstanceOf(FilterOp.class)
            .hasFieldOrPropertyWithValue("filter",
                Exprs.op(Exprs.ADD, Exprs.val(1), Exprs.op(Exprs.INDEX, Exprs.var("$"), Exprs.val(0)))
            );
        assertThat(op.getOutput())
            .isInstanceOf(ProjectOp.class)
            .hasFieldOrPropertyWithValue("projects", new Expr[]{
                Exprs.op(Exprs.INDEX, Exprs.var("$"), Exprs.val(0)),
                Exprs.op(Exprs.INDEX, Exprs.var("$"), Exprs.val(1))
            });
    }
}
