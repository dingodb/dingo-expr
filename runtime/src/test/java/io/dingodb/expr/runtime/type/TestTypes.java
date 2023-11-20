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

package io.dingodb.expr.runtime.type;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTypes {
    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            // null
            arguments(void.class, Types.NULL),
            // scalar types
            arguments(Integer.class, Types.INT),
            arguments(int.class, Types.INT),
            arguments(Long.class, Types.LONG),
            arguments(long.class, Types.LONG),
            arguments(Boolean.class, Types.BOOL),
            arguments(boolean.class, Types.BOOL),
            arguments(Double.class, Types.DOUBLE),
            arguments(double.class, Types.DOUBLE),
            arguments(BigDecimal.class, Types.DECIMAL),
            arguments(String.class, Types.STRING),
            arguments(byte[].class, Types.BYTES),
            // date&time types
            arguments(Date.class, Types.DATE),
            arguments(Time.class, Types.TIME),
            arguments(Timestamp.class, Types.TIMESTAMP),
            // collection types
            arguments(Integer[].class, Types.ARRAY_INT),
            arguments(int[].class, Types.ARRAY_INT),
            arguments(Long[].class, Types.ARRAY_LONG),
            arguments(long[].class, Types.ARRAY_LONG),
            arguments(Boolean[].class, Types.ARRAY_BOOL),
            arguments(boolean[].class, Types.ARRAY_BOOL),
            arguments(Double[].class, Types.ARRAY_DOUBLE),
            arguments(double[].class, Types.ARRAY_DOUBLE),
            arguments(BigDecimal[].class, Types.ARRAY_DECIMAL),
            arguments(String[].class, Types.ARRAY_STRING),
            arguments(Object[].class, Types.ARRAY_ANY),
            arguments(List.class, Types.LIST_ANY),
            arguments(LinkedList.class, Types.LIST_ANY),
            arguments(ArrayList.class, Types.LIST_ANY),
            arguments(Map.class, Types.MAP_ANY_ANY),
            arguments(HashMap.class, Types.MAP_ANY_ANY),
            arguments(TreeMap.class, Types.MAP_ANY_ANY),
            arguments(LinkedHashMap.class, Types.MAP_ANY_ANY),
            // any
            arguments(TestTypes.class, Types.ANY),
            arguments(Object.class, Types.ANY)
        );
    }

    private static @NonNull Stream<Arguments> getToStringParameters() {
        return Stream.of(
            arguments(Types.NULL, NullType.NAME),
            arguments(Types.array(Types.DOUBLE), "ARRAY<DOUBLE>"),
            arguments(Types.list(Types.DOUBLE), "LIST<DOUBLE>"),
            arguments(Types.map(Types.STRING, Types.LONG), "MAP<STRING, LONG>"),
            arguments(Types.tuple(new Type[]{Types.INT, Types.LONG}), "TUPLE[INT, LONG]")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testClassType(Class<?> clazz, Type type) {
        assertEquals(Types.classType(clazz), type);
    }

    @ParameterizedTest
    @MethodSource("getToStringParameters")
    public void testToString(@NonNull Type type, String expected) {
        assertThat(type.toString()).isEqualTo(expected);
    }

    @Test
    public void testHashCodeUniqueness() {
        Type[] types = new Type[]{
            Types.NULL,
            Types.INT,
            Types.LONG,
            Types.FLOAT,
            Types.DOUBLE,
            Types.BOOL,
            Types.DECIMAL,
            Types.STRING,
            Types.BYTES,
            Types.DATE,
            Types.TIME,
            Types.TIMESTAMP,
            Types.ANY,
            Types.ARRAY_INT,
            Types.ARRAY_LONG,
            Types.ARRAY_FLOAT,
            Types.ARRAY_DOUBLE,
            Types.ARRAY_BOOL,
            Types.ARRAY_DECIMAL,
            Types.ARRAY_STRING,
            Types.ARRAY_BYTES,
            Types.ARRAY_DATE,
            Types.ARRAY_TIME,
            Types.ARRAY_TIMESTAMP,
            Types.ARRAY_ANY,
            Types.LIST_INT,
            Types.LIST_LONG,
            Types.LIST_FLOAT,
            Types.LIST_DOUBLE,
            Types.LIST_BOOL,
            Types.LIST_DECIMAL,
            Types.LIST_STRING,
            Types.LIST_BYTES,
            Types.LIST_DATE,
            Types.LIST_TIME,
            Types.LIST_TIMESTAMP,
            Types.LIST_ANY,
        };
        Set<Integer> codeSet = new HashSet<>();
        for (Type type : types) {
            assertTrue(codeSet.add(type.hashCode()));
        }
    }
}
