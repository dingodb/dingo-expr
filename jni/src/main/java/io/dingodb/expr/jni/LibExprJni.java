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

import java.nio.file.Paths;

public class LibExprJni {
    public static final LibExprJni INSTANCE = new LibExprJni();

    private LibExprJni() {
        String name = System.getProperty("os.name").toLowerCase();
        String path = Paths.get("").toAbsolutePath().getParent().toString();
        if (name.indexOf("win") >= 0) {
            System.load(path + "/jni/src/main/cpp/build/libexpr_jni.dll");
        } else if (name.indexOf("mac") >= 0) {
            System.load(path + "/jni/src/main/cpp/build/libexpr_jni.dylib");
        } else {
            System.load(path + "/jni/src/main/cpp/build/libexpr_jni.so");
        }
    }

    public native Object decode(final byte[] exprBytes);

    public native void bindTuple(Object handle, final byte[] tupleBytes);

    public native Object run(Object handle);

    public native void release(Object handle);
}
