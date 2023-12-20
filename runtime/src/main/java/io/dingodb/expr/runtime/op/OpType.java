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

import lombok.Getter;

public enum OpType {
    INDEX(OpSymbol.ARRAY, 0),
    FUN(OpSymbol.FUN, 0),
    AGG(OpSymbol.FUN, 0),
    CAST(OpSymbol.FUN, 0),
    POS(OpSymbol.POS, 1),
    NEG(OpSymbol.NEG, 1),
    ADD(OpSymbol.ADD, 3),
    SUB(OpSymbol.SUB, 3),
    MUL(OpSymbol.MUL, 2),
    DIV(OpSymbol.DIV, 2),
    LT(OpSymbol.LT, 4),
    LE(OpSymbol.LE, 4),
    EQ(OpSymbol.EQ, 4),
    GT(OpSymbol.GT, 4),
    GE(OpSymbol.GE, 4),
    NE(OpSymbol.NE, 4),
    NOT(OpSymbol.NOT, 6),
    AND(OpSymbol.AND, 7),
    OR(OpSymbol.OR, 8);

    @Getter
    private final String symbol;
    @Getter
    private final int precedence;

    OpType(String symbol, int precedence) {
        this.symbol = symbol;
        this.precedence = precedence;
    }
}
