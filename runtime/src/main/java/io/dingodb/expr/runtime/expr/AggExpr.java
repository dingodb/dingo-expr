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

package io.dingodb.expr.runtime.expr;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface AggExpr {
    /**
     * Add a new row to aggregating context.
     *
     * @param var        the current value from the aggregating context, {@code null} means empty
     * @param rowContext the new row as an {@link EvalContext}
     * @param config     the {@link ExprConfig}
     * @return the result value
     */
    Object add(@Nullable Object var, @NonNull EvalContext rowContext, ExprConfig config);

    /**
     * Merge two aggregating context.
     *
     * @param var1   the value from the 1st aggregating context
     * @param var2   the value from the 2nd aggregating context
     * @param config the {@link ExprConfig}
     * @return the merged aggregating context
     */
    Object merge(@Nullable Object var1, @Nullable Object var2, ExprConfig config);

    /**
     * Get the output value from an empty aggregating context.
     *
     * @return the output value
     */
    Object emptyValue();
}
