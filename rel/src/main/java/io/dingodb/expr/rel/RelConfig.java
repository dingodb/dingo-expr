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

package io.dingodb.expr.rel;

import io.dingodb.expr.parser.ExprParser;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprContext;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.TupleEvalContextImpl;

public interface RelConfig {
    RelConfig DEFAULT = new RelConfig() {
    };

    default ExprParser getExprParser() {
        return ExprParser.DEFAULT;
    }

    default ExprCompiler getExprCompiler() {
        ExprCompiler ret = ExprCompiler.ADVANCED;
        ret.setExprContext(ExprContext.INVALID);
        return ret;
    }

    /**
     * Create new {@link TupleEvalContext} for a relational operator.
     *
     * @return the new {@link TupleEvalContext}
     */
    default TupleEvalContext getEvalContext() {
        return new TupleEvalContextImpl();
    }

    default CacheSupplier getCacheSupplier() {
        return CacheSupplierImpl.INSTANCE;
    }
}
