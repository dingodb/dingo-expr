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

package io.dingodb.expr.test.cases;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static io.dingodb.expr.runtime.expr.Exprs.TO_INT_C;
import static io.dingodb.expr.runtime.expr.Exprs.TO_LONG_C;
import static io.dingodb.expr.runtime.expr.Exprs.op;
import static io.dingodb.expr.test.ExprsHelper.dec;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class EvalExceptionProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
            arguments(op(TO_INT_C, (float) Integer.MAX_VALUE + 1E3f), Integer.MAX_VALUE),
            arguments(op(TO_INT_C, (float) Integer.MIN_VALUE - 1E3f), Integer.MIN_VALUE),
            arguments(op(TO_INT_C, (double) Integer.MAX_VALUE + 1), Integer.MAX_VALUE),
            arguments(op(TO_INT_C, (double) Integer.MIN_VALUE - 1), Integer.MIN_VALUE),
            arguments(op(TO_INT_C, dec((long) Integer.MAX_VALUE + 1L)), Integer.MIN_VALUE),
            arguments(op(TO_INT_C, dec((long) Integer.MIN_VALUE - 1L)), Integer.MAX_VALUE),
            arguments(op(TO_LONG_C, (float) Long.MAX_VALUE + 1E12f), Long.MAX_VALUE),
            arguments(op(TO_LONG_C, (float) Long.MIN_VALUE - 1E12f), Long.MIN_VALUE),
            arguments(op(TO_LONG_C, (double) Long.MAX_VALUE + 1E4), Long.MAX_VALUE),
            arguments(op(TO_LONG_C, (double) Long.MIN_VALUE - 1E4), Long.MIN_VALUE),
            arguments(op(TO_LONG_C, BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE)), Long.MIN_VALUE),
            arguments(op(TO_LONG_C, BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE)), Long.MAX_VALUE)
        );
    }
}
