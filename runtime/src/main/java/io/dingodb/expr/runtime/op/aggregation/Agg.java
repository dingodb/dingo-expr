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

import io.dingodb.expr.runtime.ExprConfig;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface Agg {
    /**
     * Merge two aggregating context.
     *
     * @param var1   the value from the 1st aggregating context
     * @param var2   the value from the 2nd aggregating context
     * @param config the {@link ExprConfig}
     * @return the merged aggregating context
     */
    Object merge(@NonNull Object var1, @NonNull Object var2, ExprConfig config);

    /**
     * Get the output value from an empty aggregating context.
     *
     * @return the output value
     */
    Object emptyValue();
}
