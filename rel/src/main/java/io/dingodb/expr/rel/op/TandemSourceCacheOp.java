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

import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.SourceOp;
import io.dingodb.expr.rel.TandemOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.stream.Stream;

final class TandemSourceCacheOp extends TandemOp implements SourceOp {
    private static final long serialVersionUID = 7810444579907423867L;

    TandemSourceCacheOp(SourceOp input, CacheOp output) {
        super(input, output);
    }

    @Override
    public @NonNull Stream<Object[]> get() {
        CacheOp cacheOp = (CacheOp) output;
        ((SourceOp) input).get().forEach(cacheOp::put);
        return cacheOp.get();
    }

    @Override
    protected @NonNull TandemSourceCacheOp make(RelOp input, RelOp output) {
        return new TandemSourceCacheOp((SourceOp) input, (CacheOp) output);
    }
}
