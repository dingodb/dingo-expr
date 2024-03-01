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
import io.dingodb.expr.rel.TandemOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.stream.Stream;

public final class TandemCacheCacheOp extends TandemOp implements CacheOp {
    private static final long serialVersionUID = -4273883622644553149L;

    public TandemCacheCacheOp(CacheOp input, CacheOp output) {
        super(input, output);
    }

    @Override
    public void put(Object @NonNull [] tuple) {
        ((CacheOp) input).put(tuple);
    }

    @Override
    public @NonNull Stream<Object[]> get() {
        ((CacheOp) input).get().forEach(((CacheOp) output)::put);
        return ((CacheOp) output).get();
    }

    @Override
    public void clear() {
        ((CacheOp) output).clear();
        ((CacheOp) input).clear();
    }

    @Override
    protected @NonNull TandemCacheCacheOp make(RelOp input, RelOp output) {
        return new TandemCacheCacheOp((CacheOp) input, (CacheOp) output);
    }
}
