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

public final class OpSymbol {
    public static final String ARRAY = "[]";
    public static final String FUN = "()";
    public static final String POS = "+";
    public static final String NEG = "-";
    public static final String ADD = " + ";
    public static final String SUB = " - ";
    public static final String MUL = "*";
    public static final String DIV = "/";
    public static final String LT = " < ";
    public static final String LE = " <= ";
    public static final String EQ = " == ";
    public static final String GT = " > ";
    public static final String GE = " >= ";
    public static final String NE = " != ";
    public static final String NOT = "!";
    public static final String AND = " && ";
    public static final String OR = " || ";

    private OpSymbol() {
    }
}
