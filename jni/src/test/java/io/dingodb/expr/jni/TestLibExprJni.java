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

package io.dingodb.expr.jni;

import io.dingodb.expr.runtime.utils.CodecUtils;
import io.dingodb.expr.test.LibExprJniUtils;
import io.dingodb.expr.test.asserts.Assert;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestLibExprJni {
    @BeforeAll
    public static void setupAll() {
        LibExprJniUtils.setLibPath();
    }

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("1101", 1),                                   // 1
            arguments("2101", -1),                                  // -1
            arguments("119601", 150),                               // 150
            arguments("219601", -150),                              // -150
            arguments("13", true),                                  // true
            arguments("23", false),                                 // false
            arguments("15401F333333333333", 7.8),                   // 7.8
            arguments("15400921FB4D12D84A", 3.1415926),             // 3.1415926
            arguments("1541B1E1A300000000", 3E8),                   // 3E8
            arguments("1703616263", "abc"),                         // 'abc'
            arguments("110111018301", 2),                           // 1 + 1
            arguments("110211038301", 5),                           // 2 + 3
            arguments("120112018302", 2L),                          // 1L + 1L
            arguments("120212038302", 5L),                          // 2L + 3L
            arguments("11031104110685018301", 27),                  // 3 + 4 * 6
            arguments("110511068301110B9101", true),                // 5 + 6 = 11
            arguments("17036162631701619307", true),                // 'abc' > 'a'
            arguments("110711088301110E930111061105950152", false), // 7 + 8 > 14 && 6 < 5
            arguments("1115F021", 21L),                             // int64(21)
            arguments("230352", false),                             // false && null
            arguments("130352", null),                              // true && null
            arguments("01A101", true),                              // is_null(null)
            arguments("1101A201", true)                             // is_true(1)
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testConst(String exprCode, Object result) {
        Object handle = LibExprJni.INSTANCE.decode(CodecUtils.hexStringToBytes(exprCode));
        Assert.value(LibExprJni.INSTANCE.run(handle)).isEqualTo(result);
        LibExprJni.INSTANCE.release(handle);
    }
}
