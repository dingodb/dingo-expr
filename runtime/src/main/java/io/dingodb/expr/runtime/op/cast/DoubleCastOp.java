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
abstract class DoubleCastOp extends CastOp {
    private static final long serialVersionUID = -1395304729108735038L;

    static double doubleCast(int value) {
        return value;
    }

    static double doubleCast(long value) {
        return value;
    }

    static double doubleCast(float value) {
        return value;
    }

    static double doubleCast(double value) {
        return value;
    }

    static double doubleCast(boolean value) {
        return value ? 1.0 : 0.0;
    }

    static double doubleCast(@NonNull BigDecimal value) {
        return value.doubleValue();
    }

    static double doubleCast(String value) {
        return Double.parseDouble(value);
    }

    static double doubleCast(byte[] value) {
        if (value == null) {
            return 0;
        } else {
            try {
                String val = new String(value);
                return Double.parseDouble(val);
            } catch (Exception e) {
                return 0;
            }
        }
    }

    static @Nullable Double doubleCast(Void ignoredValue) {
        return null;
    }

    static double doubleCastWithStringCompat(String value) {
        double result = 0;
        try {
            //For efficiency.
            result = Double.parseDouble(value);
        } catch (NumberFormatException e) {
            int lastNumberPos = 0;
            if (value != null) {
                //find the last digit position.
                String v = trimDigitString(value);
                try {
                    result = Double.parseDouble(v);
                } catch (NumberFormatException e1) {
                    result = 0;
                }
            }
        }
        return result;
    }

    @Override
    public final Type getType() {
        return Types.DOUBLE;
    }
}
