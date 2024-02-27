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

package io.dingodb.expr.runtime.op.string;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.utils.PatternUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

@Operators
abstract class ConvertPattern2Fun extends BinaryStringStringFun {
    public static final String NAME = "$CP";

    private static final long serialVersionUID = -6535404891235746L;

    static @NonNull String cp(@NonNull String value0, @NonNull String value1) {
        return PatternUtils.convertSqlToRegex(value0, value1.charAt(0));
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
