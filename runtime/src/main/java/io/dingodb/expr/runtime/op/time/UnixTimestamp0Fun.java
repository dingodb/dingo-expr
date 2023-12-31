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

package io.dingodb.expr.runtime.op.time;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.NullaryOp;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

public class UnixTimestamp0Fun extends NullaryOp {
    public static final UnixTimestamp0Fun INSTANCE = new UnixTimestamp0Fun();
    public static final String NAME = "UNIX_TIMESTAMP";

    private static final long serialVersionUID = 2891254339801474600L;

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        return DateTimeUtils.toSecond(System.currentTimeMillis(), 0).longValue();
    }
}
