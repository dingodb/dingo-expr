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

package io.dingodb.expr.test;

import java.lang.reflect.Field;

public final class LibExprJniUtils {
    private static final String LIB_PATH = "../jni/src/main/cpp/build";

    private LibExprJniUtils() {
    }

    public static void setLibPath() {
        String property = System.getProperty("user.home");
        /*System.setProperty("java.library.path", System.getProperty("java.library.path") + ":" + LIB_PATH);
        try {
            // Refresh lib path.
            Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
            fieldSysPath.setAccessible(true);
            fieldSysPath.set(null, null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }*/
    }
}
