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

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.exception.NeverRunHere;
import io.dingodb.expr.runtime.exception.SingleValueException;
import io.dingodb.expr.runtime.expr.Expr;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SingleValueAgg extends UnaryAgg {
    public static final String NAME = "SINGLE_VALUE_AGG";

    public static final SingleValueAgg INSTANCE = new SingleValueAgg(null);

    private static final long serialVersionUID = 1272593999607684823L;

    @Getter
    private final Type type;

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public @NonNull Long merge(@NonNull Object var1, @NonNull Object var2, ExprConfig config) {
        throw new NeverRunHere();
    }

    @Override
    public @Nullable Object emptyValue() {
        return null;
    }

    @Override
    public Object first(@Nullable Object value, ExprConfig config) {
        return value;
    }

    @Override
    public Object add(@Nullable Object var, @Nullable Object value, ExprConfig config) {
        throw new SingleValueException();
    }

    @Override
    public @NonNull Expr compile(@NonNull Expr operand, @NonNull ExprConfig config) {
        return new SingleValueAgg(operand.getType()).createExpr(operand);
    }
}
