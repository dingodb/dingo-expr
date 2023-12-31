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

package io.dingodb.expr.runtime.compiler;

import io.dingodb.expr.runtime.expr.Val;

import java.util.Map;
import java.util.TreeMap;

public final class ConstFactory {
    public static final ConstFactory INSTANCE = new ConstFactory();

    private final Map<String, Val> constDefinitions;

    private ConstFactory() {
        constDefinitions = new TreeMap<>(String::compareToIgnoreCase);
        constDefinitions.put("NULL", Val.NULL);
        constDefinitions.put("TRUE", Val.TRUE);
        constDefinitions.put("FALSE", Val.FALSE);
        constDefinitions.put("TAU", Val.TAU);
        constDefinitions.put("E", Val.E);
    }

    /**
     * Get a {@link Val} by its name.
     *
     * @param name the name
     * @return the const value
     */
    public Val getConst(String name) {
        return constDefinitions.get(name);
    }
}
