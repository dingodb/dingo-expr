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
import io.dingodb.expr.runtime.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;

public interface Expr extends Serializable {
    /**
     * Evaluate the result of this {@link Expr} in a specified {@link EvalContext} and {@link ExprConfig}.
     *
     * @param context the specified {@link EvalContext}, containing variables
     * @param config  the specified {@link ExprConfig}, containing eval config, like time zone.
     * @return the result
     */
    Object eval(EvalContext context, ExprConfig config);

    default Object eval() {
        return eval(null, null);
    }

    Type getType();

    @NonNull Expr simplify();

    <R, T> R accept(@NonNull ExprVisitor<R, T> visitor, T obj);
}
