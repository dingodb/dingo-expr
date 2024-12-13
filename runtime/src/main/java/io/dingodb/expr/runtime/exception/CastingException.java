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

package io.dingodb.expr.runtime.exception;

import io.dingodb.expr.common.type.Type;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CastingException extends ExprEvaluatingException {
    private static final long serialVersionUID = 7400509568348847209L;

    public CastingException(@NonNull Type toType, @NonNull Type fromType, @NonNull Throwable cause) {
        super("Casting from " + fromType + " to " + toType + " failed: " + cause.getLocalizedMessage());
    }
}
