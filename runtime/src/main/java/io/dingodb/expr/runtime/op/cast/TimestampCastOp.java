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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;

//@Operators
abstract class TimestampCastOp extends CastOp {
    private static final long serialVersionUID = -2472358326023215685L;

    static @NonNull Timestamp timestampCast(int value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Timestamp timestampCast(long value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Timestamp timestampCast(Date value, ExprConfig config) {
        LocalDateTime ts =  new Timestamp(value.getTime()).toLocalDateTime();
        TimeZone timeZone = (config != null ? config.getTimeZone() : TimeZone.getDefault());
        ZonedDateTime zonedDateTime = ts.atZone(timeZone.toZoneId());

        java.time.ZoneOffset zoneOffset = zonedDateTime.getOffset();
        ZonedDateTime targetZonedDateTime = zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
        return new Timestamp(targetZonedDateTime.toLocalDateTime().toInstant(zoneOffset).toEpochMilli());
    }

    static @NonNull Timestamp timestampCast(@NonNull BigDecimal value) {
        return new Timestamp(DateTimeUtils.fromSecond(value));
    }

    static @NonNull Timestamp timestampCast(@NonNull Time value) {
        int hours = value.toLocalTime().getHour();
        int minutes = value.toLocalTime().getMinute();
        int seconds = value.toLocalTime().getSecond();
        LocalDateTime localDatetime = LocalDate.now().atTime(hours,minutes,seconds);
        return Timestamp.valueOf(localDatetime);
    }

    static @Nullable Timestamp timestampCast(String value, @NonNull ExprConfig config) {
        return DateTimeUtils.parseTimestamp(value, config.getParseDateAndTimestampFormatters());
    }

    static @NonNull Timestamp timestampCast(@NonNull Timestamp value) {
        return value;
    }

    static @Nullable Timestamp timestampCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.TIMESTAMP;
    }
}
