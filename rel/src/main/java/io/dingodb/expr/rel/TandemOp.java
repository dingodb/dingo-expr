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
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode(of = {"input", "output"})
public abstract class TandemOp implements RelOp {
    private static final long serialVersionUID = -2035098988502010221L;

    @Getter
    protected final RelOp input;
    @Getter
    protected final RelOp output;

    protected abstract TandemOp make(RelOp input, RelOp output);

    @Override
    public @NonNull TandemOp compile(@NonNull TupleCompileContext context, @NonNull RelConfig config) {
        RelOp newInput = input.compile(context, config);
        RelOp newOutput = output.compile(context.withType(newInput.getType()), config);
        return make(newInput, newOutput);
    }

    @Override
    public TupleType getType() {
        return output.getType();
    }

    @Override
    public <R, T> R accept(@NonNull RelOpVisitor<R, T> visitor, T obj) {
        return visitor.visitTandemOp(this, obj);
    }

    @Override
    public String toString() {
        return input + ", " + output;
    }
}
