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
import io.dingodb.expr.common.timezone.DateTimeUtils;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.op.UnaryNumericOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Operators
abstract class FromUnixTimeFun extends UnaryNumericOp {
    public static final String NAME = "FROM_UNIXTIME";

    private static final long serialVersionUID = 1441707237891012175L;

    static @NonNull Timestamp fromUnixTime(int value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Timestamp fromUnixTime(long value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Timestamp fromUnixTime(@NonNull BigDecimal value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    @Override
    public final Type getType() {
        return Types.TIMESTAMP;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
