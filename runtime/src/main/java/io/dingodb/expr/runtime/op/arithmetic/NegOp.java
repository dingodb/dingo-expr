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

package io.dingodb.expr.runtime.op.arithmetic;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.op.UnaryNumericOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

@Operators
abstract class NegOp extends UnaryNumericOp {
    private static final long serialVersionUID = -8440534198682078139L;

    static int neg(int value) {
        return -value;
    }

    static long neg(long value) {
        return -value;
    }

    static float neg(float value) {
        return -value;
    }

    static double neg(double value) {
        return -value;
    }

    static @NonNull BigDecimal neg(@NonNull BigDecimal value) {
        return value.negate();
    }

    @Override
    public final @NonNull OpType getOpType() {
        return OpType.NEG;
    }
}
