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
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.sql.Date;
import java.sql.Timestamp;

@Operators
public class QuarterFun extends UnaryOp {
    public static final String NAME = "QUARTER";
    @Serial
    private static final long serialVersionUID = -5306012511147355441L;

    static int extractQuarter(@NonNull Date value, ExprConfig config) {
        return DateTimeUtils.extractQuarter(value);
    }

    static int extractQuarter(@NonNull Timestamp value, ExprConfig config) {
        return DateTimeUtils.extractQuarter(value);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
