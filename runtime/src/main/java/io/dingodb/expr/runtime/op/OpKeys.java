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

package io.dingodb.expr.runtime.op;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;

public final class OpKeys {
    public static final DefaultOpKeys DEFAULT = new DefaultOpKeys();
    public static final BinaryOtherOpKeys INTERVAL = new BinaryOtherOpKeys();
    public static final SameTypeOpKeys ALL_BOOL = new SameTypeOpKeys(Types.BOOL);
    public static final SameTypeOpKeys ALL_LONG = new SameTypeOpKeys(Types.LONG);
    public static final SameTypeOpKeys ALL_DOUBLE = new SameTypeOpKeys(Types.DOUBLE);
    public static final SameTypeOpKeys ALL_STRING = new SameTypeOpKeys(Types.STRING);
    public static final SameTypeOpKeys ALL_DATE = new SameTypeOpKeys(Types.DATE);
    public static final SameTypeOpKeys ALL_TIME = new SameTypeOpKeys(Types.TIME);
    public static final SameTypeOpKeys ALL_TIMESTAMP = new SameTypeOpKeys(Types.TIMESTAMP);
    public static final BinaryOpKeys STRING_INT = new BinaryOpKeys(Types.STRING, Types.INT);
    public static final BinaryOpKeys DECIMAL_INT = new BinaryOpKeys(Types.DECIMAL, Types.INT);
    public static final BinaryOpKeys DATE_STRING = new BinaryOpKeys(Types.DATE, Types.STRING);
    public static final BinaryOpKeys DATE_LONG = new BinaryOpKeys(Types.DATE, Types.LONG);
    public static final BinaryOpKeys TIME_STRING = new BinaryOpKeys(Types.TIME, Types.STRING);
    public static final BinaryOpKeys TIMESTAMP_STRING = new BinaryOpKeys(Types.TIMESTAMP, Types.STRING);
    public static final TertiaryOpKeys STRING_INT_INT = new TertiaryOpKeys(Types.STRING, Types.INT, Types.INT);
    public static final TertiaryOpKeys STRING_STRING_INT = new TertiaryOpKeys(Types.STRING, Types.STRING, Types.INT);

    private OpKeys() {
    }

    @SuppressWarnings("MethodMayBeStatic")
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class DefaultOpKeys {
        public @Nullable OpKey keyOf(Type type0) {
            return type0;
        }

        public @Nullable OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
            if (type0.matches(type1)) {
                return type0;
            } else if (type0.equals(Types.NULL)) {
                return type1;
            } else if (!type0.matches(type1)) {
                return Types.ANY;
            }
            return null;
        }

        public @Nullable OpKey keyOf(@NonNull Type type0, @NonNull Type type1, @NonNull Type type2) {
            if (type0.matches(type1)) {
                if (type0.matches(type2)) {
                    return type0;
                } else if (type0.equals(Types.NULL)) {
                    return type2;
                }
            } else if (type0.equals(Types.NULL)) {
                return keyOf(type1, type2);
            }
            return null;
        }

        public @Nullable OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
            Type best = Types.NULL;
            for (Type type : types) {
                best = (type.numericPrecedence() > best.numericPrecedence() ? type : best);
            }
            if (best.isNumeric()) {
                // Convert null/bool to int.
                best = (!Types.BOOL.matches(best)) ? best : Types.INT;
                Arrays.fill(types, best);
                return best;
            }
            return null;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class SameTypeOpKeys {
        private final Type type;

        public @Nullable OpKey keyOf(Type type0) {
            if (this.type.matches(type0)) {
                return type;
            }
            return null;
        }

        public @Nullable OpKey keyOf(Type type0, Type type1) {
            if (this.type.matches(type0) && this.type.matches(type1)) {
                return type;
            }
            return null;
        }

        public @Nullable OpKey keyOf(Type type0, Type type1, Type type2) {
            if (this.type.matches(type0) && this.type.matches(type1) && this.type.matches(type2)) {
                return type;
            }
            return null;
        }

        public @Nullable OpKey keyOf(Type... types) {
            return Arrays.stream(types).allMatch(this.type::matches) ? type : null;
        }

        public OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
            Arrays.fill(types, type);
            return type;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class BinaryOpKeys {
        private final Type type0;
        private final Type type1;

        public @Nullable OpKey keyOf(Type type0, Type type1) {
            if (this.type0.matches(type0) && this.type1.matches(type1)) {
                return type0;
            }
            return null;
        }

        public @Nullable OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
            types[0] = type0;
            types[1] = type1;
            return type0;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class BinaryOtherOpKeys {

        private static final List<Type> types0 = Arrays.asList(
            Types.DATE,
            Types.TIME,
            Types.TIMESTAMP);

        private static final List<Type> types1 = Arrays.asList(
            Types.INTERVAL_YEAR,
            Types.YEAR,
            Types.INTERVAL_MONTH,
            Types.MONTH,
            Types.INTERVAL_DAY,
            Types.DAY,
            Types.INTERVAL_WEEK,
            Types.WEEK,
            Types.INTERVAL_DAY_TIME,
            Types.INTERVAL_HOUR,
            Types.HOUR,
            Types.INTERVAL_MINUTE,
            Types.MINUTE,
            Types.INTERVAL_SECOND,
            Types.SECOND);

        public @Nullable OpKey keyOf(Type type0, Type type1) {
            return types0.stream().anyMatch(type0::matches)
                ? (types1.stream().anyMatch(type1::matches) ? Types.tuple(type0, type1) : null) : null;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class TertiaryOpKeys {
        private final Type type0;
        private final Type type1;
        private final Type type2;

        public @Nullable OpKey keyOf(Type type0, Type type1, Type type2) {
            if (this.type0.matches(type0) && this.type1.matches(type1) && this.type2.matches(type2)) {
                return type0;
            }
            return null;
        }

        public @Nullable OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
            types[0] = type0;
            types[1] = type1;
            types[2] = type2;
            return type0;
        }
    }
}
