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

package io.dingodb.expr.runtime.op.collection;

import io.dingodb.expr.common.type.ArrayType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.Val;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SliceArrayOfTupleOp extends SliceArrayOp {
    private static final long serialVersionUID = 2417363233945832611L;

    SliceArrayOfTupleOp(ArrayType originalType, ArrayType type) {
        super(originalType, type);
    }

    @Override
    Object getValueOf(Object value, int index) {
        return ((Object[]) value)[index];
    }

    @Override
    public @NonNull Expr simplify(@NonNull BinaryOpExpr expr, ExprConfig config) {
        Type elementType = guessElementType(expr, config);
        if (elementType != null) {
            if (!elementType.equals(type.getElementType())) {
                return Exprs.op(new SliceArrayOfTupleOp(
                    (ArrayType) originalType,
                    Types.array(elementType)
                ), expr.getOperand0(), expr.getOperand1());
            }
            return expr;
        }
        return Val.NULL;
    }
}
