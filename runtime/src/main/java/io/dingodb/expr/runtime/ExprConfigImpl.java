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

package io.dingodb.expr.runtime;

import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;

import java.time.ZoneId;

public class ExprConfigImpl implements ExprConfig {
    private ExprContext exprContext = ExprContext.INVALID;
    private DingoTimeZoneProcessor processor = new DingoTimeZoneProcessor(ZoneId.systemDefault());

    @Override
    public boolean withSimplification() {
        return true;
    }

    @Override
    public boolean withRangeCheck() {
        return true;
    }

    public ExprContext getExprContext() {
        return exprContext;
    }

    public void setExprContext(ExprContext exprContext) {
        this.exprContext = exprContext;
    }

    @Override
    public DingoTimeZoneProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(DingoTimeZoneProcessor processor) {
        this.processor = processor;
    }
}
