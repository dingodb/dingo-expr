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
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Operators
abstract class YearFun extends UnaryOp {
    public static final String NAME = "YEAR";
    private static final long serialVersionUID = 3443110114993243173L;

    static int extractYear(@NonNull Date value, @NonNull ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();

        return processor.extractYear(value);
    }

    static int extractYear(@NonNull Timestamp value, @NonNull ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();

        return processor.extractYear(value);
    }

    static Integer extractYear(String value, @NonNull ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();

        return processor.extractYear(value);
    }

    static @Nullable Integer extractYear(Time value, @NonNull ExprConfig config) {
        return null;
    }

    static @Nullable Object extractYear(Void value, @NonNull ExprConfig config) {
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

}
