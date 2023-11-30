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
abstract class PosOp extends UnaryNumericOp {
    private static final long serialVersionUID = -6665715127982088591L;

    static int pos(int value) {
        return value;
    }

    static long pos(long value) {
        return value;
    }

    static float pos(float value) {
        return value;
    }

    static double pos(double value) {
        return value;
    }

    static @NonNull BigDecimal pos(@NonNull BigDecimal value) {
        return value;
    }

    @Override
    public final @NonNull OpType getOpType() {
        return OpType.POS;
    }
}
