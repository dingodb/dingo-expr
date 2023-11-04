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

package io.dingodb.expr.test.asserts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

public class AssertValue extends Assert<Object> {
    AssertValue(Object value) {
        super(value);
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertValue isEqualTo(Object expected) {
        if (expected == null) {
            assertThat(instance).isNull();
        } else if (instance instanceof Float) {
            assertThat((Float) instance).isCloseTo((Float) expected, offset(1E-6f));
        } else if (instance instanceof Double) {
            assertThat((Double) instance).isCloseTo((Double) expected, offset(1E-6));
        } else {
            assertThat(instance).isEqualTo(expected);
        }
        return this;
    }
}
