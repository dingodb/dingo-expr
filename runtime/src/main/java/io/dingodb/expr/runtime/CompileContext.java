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

package io.dingodb.expr.runtime;

import io.dingodb.expr.runtime.type.Type;

public interface CompileContext {
    /**
     * Get the variable id of this context if it stands for a variable, or {@code null}.
     *
     * @return the id or {@code null}
     */
    default Object getId() {
        return null;
    }

    /**
     * Get the type of this context if it stands for a variable, or {@code null}.
     *
     * @return the type
     */
    default Type getType() {
        return null;
    }

    /**
     * Get a specified child context of this context. A {@link CompileContext} may have child contexts.
     *
     * @param index then index of the child, can be a {@link String} or {@link Integer}
     * @return the child context
     */
    default CompileContext getChild(Object index) {
        return null;
    }
}
