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

package io.dingodb.expr.json.runtime;

import org.checkerframework.checker.nullness.qual.NonNull;

public interface DataSchemaVisitor<R, T> {
    default R visit(@NonNull DataSchema schema) {
        return schema.accept(this, null);
    }

    default R visit(@NonNull DataSchema schema, T obj) {
        return schema.accept(this, obj);
    }

    R visitLeaf(@NonNull DataLeaf schema, T obj);

    R visitTuple(@NonNull DataTuple schema, T obj);

    R visitDict(@NonNull DataDict schema, T obj);
}
