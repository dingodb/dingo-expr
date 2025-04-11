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

package io.dingodb.expr.runtime.op.cast;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.TimeZone;

@Operators
abstract class DateCastOp extends CastOp {
    private static final long serialVersionUID = -5795485462775179517L;

    static @NonNull Date dateCast(int value) {
        return new Date(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Date dateCast(long value) {
        return new Date(DateTimeUtils.fromSecond(value));
    }

    static @Nullable Date dateCast(String value, @NonNull ExprConfig config) {
        try {
            return DateTimeUtils.parseDate(value, config.getParseDateFormatters(), config.getTimeZone().toZoneId());
        } catch (Exception e) {
            return DateTimeUtils.parseDate(
                value, DateTimeUtils.DEFAULT_PARSE_TIMESTAMP_FORMATTERS, config.getTimeZone().toZoneId());
        }
    }

    static @NonNull Date dateCast(@NonNull Date value) {
        return value;
    }

    static @NonNull Date dateCast(@NonNull Timestamp value) {
        return new Date(value.getTime());
    }

    static @Nullable Date dateCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.DATE;
    }
}
