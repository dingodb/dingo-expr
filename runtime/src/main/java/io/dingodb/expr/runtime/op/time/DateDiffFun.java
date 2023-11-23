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

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.util.Arrays;

@Operators
abstract class DateDiffFun extends BinaryOp {
    public static final String NAME = "DATEDIFF";

    private static final long serialVersionUID = -8589287644033616224L;

    static long dateDiff(@NonNull Date value0, @NonNull Date value1) {
        return DateTimeUtils.dateDiff(value0, value1);
    }

    @Override
    public Object keyOf(@NonNull Type type0, @NonNull Type type1) {
        if (Types.DATE.matches(type0) && Types.DATE.matches(type1)) {
            return Types.DATE;
        }
        return null;
    }

    @Override
    public Object bestKeyOf(@NonNull Type @NonNull [] types) {
        Arrays.fill(types, Types.DATE);
        return Types.DATE;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
