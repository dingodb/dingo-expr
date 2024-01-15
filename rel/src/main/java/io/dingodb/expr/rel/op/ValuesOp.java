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

package io.dingodb.expr.rel.op;

import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.SourceOp;
import io.dingodb.expr.rel.TupleCompileContext;
import io.dingodb.expr.runtime.type.TupleType;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Stream;

final class ValuesOp implements SourceOp {
    public static final String NAME = "VALUE";

    private static final long serialVersionUID = -6015415126811940690L;

    private final List<Object @NonNull []> values;

    @Getter
    private TupleType type;

    ValuesOp(List<Object @NonNull []> values) {
        this.values = values;
    }

    @Override
    public @NonNull Stream<Object[]> get() {
        return values.stream();
    }

    @Override
    public void compile(@NonNull TupleCompileContext context, @NonNull RelConfig config) {
        type = context.getType();
    }

    @Override
    public String toString() {
        return NAME;
    }
}
