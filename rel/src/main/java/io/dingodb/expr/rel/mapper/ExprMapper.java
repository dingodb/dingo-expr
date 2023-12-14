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

package io.dingodb.expr.rel.mapper;

import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.runtime.expr.Expr;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.mapstruct.Context;
import org.mapstruct.Mapper;

@SuppressWarnings("MethodMayBeStatic")
@Mapper
public abstract class ExprMapper {
    public String asString(@NonNull Expr expr) {
        return expr.toString();
    }

    public Expr asExpr(String expr, @Context @NonNull RelConfig config) throws ExprParseException {
        return config.getExprParser().parse(expr);
    }
}
