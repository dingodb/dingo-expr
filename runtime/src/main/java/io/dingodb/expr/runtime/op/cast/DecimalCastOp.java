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

package io.dingodb.expr.runtime.op.cast;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;

//@Operators
abstract class DecimalCastOp extends CastOp {
    private static final long serialVersionUID = -2269255796282496495L;

    static @NonNull BigDecimal decimalCast(int value) {
        return BigDecimal.valueOf(value);
    }

    static @NonNull BigDecimal decimalCast(long value) {
        return BigDecimal.valueOf(value);
    }

    static @NonNull BigDecimal decimalCast(float value) {
        return BigDecimal.valueOf(value);
    }

    static @NonNull BigDecimal decimalCast(double value) {
        return BigDecimal.valueOf(value);
    }

    static @NonNull BigDecimal decimalCast(boolean value) {
        return value ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    static @NonNull BigDecimal decimalCast(@NonNull BigDecimal value) {
        return value;
    }

    static @NonNull BigDecimal decimalCast(@NonNull String value) {
        return new BigDecimal(value);
    }

    static BigDecimal decimalCast(byte[] value) {
        if (value == null) {
            return null;
        } else {
            try {
                String val = new String(value);
                return new BigDecimal(val);
            } catch (Exception e) {
                return null;
            }
        }
    }

    static @Nullable BigDecimal decimalCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.DECIMAL;
    }

    static @NonNull BigDecimal decimalCastWithStringCompat(@NonNull String value) {
        BigDecimal result = null;
        String val = value.trim();
        try {
            result = new BigDecimal(val);
        } catch (NumberFormatException e) {
            int lastNumberPos = 0;
            if (value != null) {
                //find the last digit position.
                for (int i = value.length() - 1; i >= 0; i-- ) {
                    if (Character.isDigit(value.charAt(i))) {
                        lastNumberPos = i;
                        break;
                    }
                }
                String v = value.substring(0, lastNumberPos + 1);
                try {
                    result = new BigDecimal(v);
                } catch (NumberFormatException e1) {
                    result = new BigDecimal(0);
                }
            }
        }

        return result;
    }
}
