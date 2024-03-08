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

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.NullaryOpExpr;
import io.dingodb.expr.runtime.op.NullaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.TimeZone;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class CurrentDateFun extends NullaryOp {
    public static final String NAME = "CURRENT_DATE";
    public static final CurrentDateFun INSTANCE = new CurrentDateFun();

    private static final long serialVersionUID = -249163074172189517L;

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        TimeZone timeZone = (config != null ? config.getTimeZone() : TimeZone.getDefault());
        return DateTimeUtils.currentDate(timeZone);
    }

    @Override
    public boolean isConst(@NonNull NullaryOpExpr expr) {
        return false;
    }

    @Override
    public Type getType() {
        return Types.DATE;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
