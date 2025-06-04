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

package io.dingodb.expr.runtime.expr;

import io.dingodb.expr.common.type.DecimalType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@EqualsAndHashCode(of = {"id", "type"})
public class Var implements Expr {
    public static final String WHOLE_VAR = "$";

    private static final long serialVersionUID = -7434384449038456900L;

    @Getter
    private final Object id;
    @Getter
    private final Type type;

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        if (this.type instanceof DecimalType) {
            Object objVal = Objects.requireNonNull(context).get(id);
            if (!(objVal instanceof BigDecimal)) {
                return objVal;
            }
            BigDecimal obj = (BigDecimal) Objects.requireNonNull(context).get(id);
            int typeScale = (int)((DecimalType)this.type).getScale();

            if (obj == null) {
                return null;
            }

            if (typeScale > 0) {
                return obj.setScale(typeScale, RoundingMode.HALF_UP);
            }
        }
        return Objects.requireNonNull(context).get(id);
    }

    @Override
    public @NonNull Expr simplify(ExprConfig config) {
        return this;
    }

    @Override
    public <R, T> R accept(@NonNull ExprVisitor<R, T> visitor, T obj) {
        return visitor.visitVar(this, obj);
    }

    @Override
    public @NonNull String toDebugString() {
        return getClass().getSimpleName() + "[" + getId() + ", " + type + "]";
    }

    /**
     * Set the value of this variable in a specified EvalContext.
     *
     * @param context the EvalContext
     * @param value   the new value
     */
    @SuppressWarnings("unused")
    public void set(EvalContext context, Object value) {
        Objects.requireNonNull(context).set(id, value);
    }

    @Override
    public String toString() {
        if (id instanceof Integer || id instanceof Long) {
            return WHOLE_VAR + "[" + id + "]";
        }
        return id.toString();
    }
}
