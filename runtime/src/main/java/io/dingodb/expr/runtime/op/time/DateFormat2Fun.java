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
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.sql.Timestamp;

//@Operators
abstract class DateFormat2Fun extends BinaryOp {
    public static final String NAME = "DATE_FORMAT";

    private static final long serialVersionUID = -5201676038056246158L;

    static @NonNull String dateFormat(@NonNull Date value, @NonNull String format) {
        return DateTimeUtils.dateFormat(value, format);
    }

    static @NonNull String dateFormat(@NonNull Timestamp value, @NonNull String format) {
        return DateTimeUtils.timestampFormat(value, format);
    }

    static String dateFormat(Number value, @NonNull String format) {
        return null;
    }

    @Override
    public final OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        OpKey opKey = OpKeys.DATE_STRING.keyOf(type0, type1);
        return opKey == null ? OpKeys.TIMESTAMP_STRING.keyOf(type0, type1) : opKey;
    }

    @Override
    public final OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        return OpKeys.DATE_STRING.bestKeyOf(types);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
