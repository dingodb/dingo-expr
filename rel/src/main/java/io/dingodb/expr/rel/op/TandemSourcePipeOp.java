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

import io.dingodb.expr.rel.PipeOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.SourceOp;
import io.dingodb.expr.rel.TandemOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.stream.Stream;

final class TandemSourcePipeOp extends TandemOp implements SourceOp {
    private static final long serialVersionUID = 1925144325886141045L;

    TandemSourcePipeOp(SourceOp input, PipeOp output) {
        super(input, output);
    }

    @Override
    public @NonNull Stream<Object[]> get() {
        return ((SourceOp) input).get().map(((PipeOp) output)::put).filter(Objects::nonNull);
    }

    @Override
    protected @NonNull TandemSourcePipeOp make(RelOp input, RelOp output) {
        return new TandemSourcePipeOp((SourceOp) input, (PipeOp) output);
    }
}
