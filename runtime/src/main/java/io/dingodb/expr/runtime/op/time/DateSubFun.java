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
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;

@Operators
public class DateSubFun extends BinaryOp {
    public static final String NAME = "DATE_SUB";
    private static final long serialVersionUID = 3693816042300916311L;

    @Override
    protected Date evalNonNullValue(@NonNull Object value0, @NonNull Object value1,
                                    ExprConfig config) {
        Date dateVal = (Date) value0;
        Long val1 = (Long) value1;
        long newDateLongVal = dateVal.getTime() - val1;
        return new Date(newDateLongVal);
    }

    @Override
    public final OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        return OpKeys.DATE_LONG.bestKeyOf(types);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Type getType() {
        return Types.DATE;
    }

    @Override
    public OpKey getKey() {
        return keyOf(Types.DATE, Types.LONG);
    }
}
