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

package io.dingodb.expr.rel;

import io.dingodb.expr.rel.op.FilterOp;
import io.dingodb.expr.rel.op.GroupedAggregateOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.UngroupedAggregateOp;
import org.checkerframework.checker.nullness.qual.NonNull;

public class RelOpVisitorBase<R, T> implements RelOpVisitor<R, T> {
    public R visit(@NonNull RelOp op) {
        return op.accept(this, null);
    }

    public R visit(@NonNull RelOp op, T obj) {
        return op.accept(this, obj);
    }

    @Override
    public R visitSourceOp(SourceOp op, T obj) {
        return null;
    }

    @Override
    public R visitFilterOp(FilterOp op, T obj) {
        return null;
    }

    @Override
    public R visitProjectOp(ProjectOp op, T obj) {
        return null;
    }

    @Override
    public R visitTandemOp(TandemOp op, T obj) {
        return null;
    }

    @Override
    public R visitUngroupedAggregateOp(UngroupedAggregateOp op, T obj) {
        return null;
    }

    @Override
    public R visitGroupedAggregateOp(GroupedAggregateOp op, T obj) {
        return null;
    }
}
