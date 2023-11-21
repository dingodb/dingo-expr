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

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.type.CollectionType;
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
abstract class SliceOp extends SliceOpFactory {
    private static final long serialVersionUID = 7410502532140243174L;

    protected final CollectionType originalType;
    @Getter
    protected final CollectionType type;

    protected Type guessElementType(@NonNull BinaryOpExpr expr, ExprConfig config) {
        Expr operand1 = expr.getOperand1();
        if (operand1 instanceof Val) {
            Integer index = (Integer) operand1.eval(null, config);
            if (index != null) {
                // Now the type is known.
                return ((TupleType) (originalType.getElementType())).getTypes()[index];
            }
            return null;
        }
        return Types.ANY;
    }

    @Override
    public Object getKey() {
        return originalType;
    }

    abstract Object getValueOf(Object value, int index);
}
