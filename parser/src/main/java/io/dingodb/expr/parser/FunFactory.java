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

package io.dingodb.expr.parser;

import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.NullaryOp;
import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.VariadicOp;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface FunFactory {
    /**
     * Get a nullary function (Op) by its name.
     *
     * @param funName the name of the function
     * @return the function (Op)
     */
    NullaryOp getNullaryFun(@NonNull String funName);

    /**
     * Register a nullary function.
     *
     * @param funName the name of the function
     * @param op      the corresponding {@link NullaryOp}
     */
    void registerNullaryFun(@NonNull String funName, @NonNull NullaryOp op);

    /**
     * Get a unary function (Op) by its name.
     *
     * @param funName the name of the function
     * @return the function (Op)
     */
    UnaryOp getUnaryFun(@NonNull String funName);

    /**
     * Register a unary function.
     *
     * @param funName the name of the function
     * @param op      the corresponding {@link UnaryOp}
     */
    void registerUnaryFun(@NonNull String funName, @NonNull UnaryOp op);

    /**
     * Get a binary function (Op) by its name.
     *
     * @param funName the name of the function
     * @return the function (Op)
     */
    BinaryOp getBinaryFun(@NonNull String funName);

    /**
     * Register a binary function.
     *
     * @param funName the name of the function
     * @param op      the corresponding {@link BinaryOp}
     */
    void registerBinaryFun(@NonNull String funName, @NonNull BinaryOp op);

    /**
     * Get a tertiary function (Op) by its name.
     *
     * @param funName the name of the function
     * @return the function (Op)
     */
    TertiaryOp getTertiaryFun(@NonNull String funName);

    /**
     * Register a tertiary function.
     *
     * @param funName the name of the function
     * @param op      the corresponding {@link TertiaryOp}
     */
    void registerTertiaryFun(@NonNull String funName, @NonNull TertiaryOp op);

    /**
     * Get a variadic function (Op) by its name.
     *
     * @param funName the name of the function
     * @return the function (Op)
     */
    VariadicOp getVariadicFun(@NonNull String funName);

    /**
     * Register a variadic function.
     *
     * @param funName the name of the function
     * @param op      the corresponding {@link VariadicOp}
     */
    void registerVariadicFun(@NonNull String funName, @NonNull VariadicOp op);
}
