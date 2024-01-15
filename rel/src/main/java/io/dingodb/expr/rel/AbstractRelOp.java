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

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.type.TupleType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

@EqualsAndHashCode(of = {"type"})
public abstract class AbstractRelOp implements RelOp {
    private static final long serialVersionUID = -7066415039209226040L;

    protected transient TupleEvalContext evalContext;
    protected transient ExprConfig exprConfig;

    @Getter
    protected TupleType type;

    @Override
    public void compile(TupleCompileContext context, @NonNull RelConfig config) {
        evalContext = config.getEvalContext();
        exprConfig = config.getExprCompiler().getConfig();
    }
}
