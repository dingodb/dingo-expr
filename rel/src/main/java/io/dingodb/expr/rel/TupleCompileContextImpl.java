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

package io.dingodb.expr.rel;

import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.ExprContext;
import io.dingodb.expr.runtime.ExprPushdownCond;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class TupleCompileContextImpl implements TupleCompileContext {
    @Getter
    private final TupleType type;

    private ExprContext exprContext;

    private ExprPushdownCond exprPushdownCond;
    private boolean notPushdown;

    @Override
    public CompileContext getChild(Object index) {
        return new CompileContext() {
            @Override
            public Object getId() {
                return index;
            }

            @Override
            public Type getType() {
                return type.getTypes()[(Integer) index];
            }
        };
    }

    @Override
    public TupleCompileContext withType(TupleType type) {
        return new TupleCompileContextImpl(type);
    }

    @Override
    public ExprContext getExprContext() {
        return exprContext;
    }

    @Override
    public void setExprContext(ExprContext exprContext) {
        this.exprContext = exprContext;
    }

    @Override
    public ExprPushdownCond getExprPushdownCond() {
        return exprPushdownCond;
    }

    @Override
    public void setExprPushdownCond(ExprPushdownCond exprPushdownCond) {
        this.exprPushdownCond = exprPushdownCond;
    }

    @Override
    public boolean getNotPushdown() {
        return notPushdown;
    }

    @Override
    public void setNotPushdown(boolean notPushdown) {
        this.notPushdown = notPushdown;
    }
}
