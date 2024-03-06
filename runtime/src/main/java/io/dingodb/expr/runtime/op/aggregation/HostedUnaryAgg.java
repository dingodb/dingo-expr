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

package io.dingodb.expr.runtime.op.aggregation;

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.type.Type;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class HostedUnaryAgg extends UnaryAgg {
    private static final long serialVersionUID = -8820539767711142430L;

    protected final BinaryOp hostedOp;

    @Override
    public final @NonNull Expr compile(@NonNull Expr operand, @NonNull ExprConfig config) {
        Type type = operand.getType();
        BinaryOp op = hostedOp.getOp(hostedOp.keyOf(type, type));
        return host(op).createExpr(operand);
    }

    protected abstract HostedUnaryAgg host(BinaryOp op);

    @Override
    public final Type getType() {
        return hostedOp.getType();
    }

    @Override
    public Object first(@Nullable Object value, ExprConfig config) {
        return value;
    }

    @Override
    public final Object add(@Nullable Object var, @Nullable Object value, ExprConfig config) {
        if (var != null) {
            if (value != null) {
                return hostedOp.evalValue(var, value, config);
            }
            return var;
        }
        return value;
    }

    @Override
    public final Object merge(@NonNull Object var1, @NonNull Object var2, ExprConfig config) {
        return hostedOp.evalValue(var1, var2, config);
    }

    @Override
    public Object emptyValue() {
        return null;
    }
}
