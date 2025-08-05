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
abstract class FloatCastOp extends CastOp {
    private static final long serialVersionUID = -5519870437688956825L;

    static float floatCast(int value) {
        return value;
    }

    static float floatCast(long value) {
        return value;
    }

    static float floatCast(float value) {
        return value;
    }

    static float floatCast(double value) {
        return (float) value;
    }

    static float floatCast(boolean value) {
        return value ? 1.0f : 0.0f;
    }

    static float floatCast(@NonNull BigDecimal value) {
        return value.floatValue();
    }

    static float floatCast(@NonNull String value) {
        return Float.parseFloat(value);
    }

    static float floatCast(byte[] value) {
        if (value == null) {
            return 0;
        } else {
            try {
                String val = new String(value);
                return Float.parseFloat(val);
            } catch (Exception e) {
                return 0;
            }
        }
    }

    static @Nullable Float floatCast(Void ignoredValue) {
        return null;
    }

    @Override
    public final Type getType() {
        return Types.FLOAT;
    }

    static float floatCastWithStringCompat(@NonNull String value) {
        float result = 0;
        String val = value.trim();
        try {
            result = Float.parseFloat(val);
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
                    result = Float.parseFloat(value.trim());
                } catch (NumberFormatException e1) {
                    result = 0;
                }
            }
        }

        return result;
    }
}
