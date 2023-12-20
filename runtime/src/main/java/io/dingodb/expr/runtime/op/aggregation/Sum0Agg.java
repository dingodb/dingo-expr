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

package io.dingodb.expr.runtime.op.aggregation;

import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.op.BinaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

public class Sum0Agg extends SumAgg {
    public static final String NAME = "SUM0";

    public static final Sum0Agg INSTANCE = new Sum0Agg(Exprs.ADD);

    private static final long serialVersionUID = 9015283371751473848L;

    private Sum0Agg(BinaryOp op) {
        super(op);
    }

    @Override
    public Object emptyValue() {
        return Exprs.val(0, getType()).eval();
    }

    @Override
    protected Sum0Agg host(BinaryOp op) {
        return new Sum0Agg(op);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
