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

package io.dingodb.expr.runtime.op.date;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
abstract class HourFun extends UnaryOp {
    public static final String NAME = "HOUR";

    private static final long serialVersionUID = 6933366854310441077L;

    static int extractHour(@NonNull Date value, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DingoDateTime dateTime = processor.getTierProcessor().convertInput(value, DateTimeType.TIMESTAMP);

        return processor.extractHour(dateTime);
    }

    static int extractHour(@NonNull Time value, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();

        return processor.extractHour(value);
    }

    static int extractHour(@NonNull Timestamp value, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();

        return processor.extractHour(value);
    }

    static Integer extractHour(String value, @NonNull ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        DateTimeType inputType = processor.inferInputType(value);
        if (inputType == null) {
            // Illegal string returns null
            return null;
        }
        DingoDateTime dateTime = processor.getTierProcessor().convertInput(value, DateTimeType.TIMESTAMP);
        if (dateTime == null) {
            dateTime = processor.getTierProcessor().convertInput(value, DateTimeType.TIME);
        }

        return processor.extractHour(dateTime);
    }

    static @Nullable Object extractHour(Void value, @NonNull ExprConfig config) {
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
