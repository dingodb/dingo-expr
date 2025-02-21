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

package io.dingodb.expr.common.type;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

public class IntervalWeekType extends IntervalType {

    @NoArgsConstructor
    @AllArgsConstructor
    public static class IntervalWeek extends IntervalWeekType {
        public BigDecimal value;
        public Type elementType;

        @Override
        public boolean equals(Object obj) {
            return obj instanceof IntervalWeek;
        }
    }

    public static final String NAME = "INTERVAL_WEEK";
    private static final int CODE = 207;

    IntervalWeekType() {
        super();
    }

    @Override
    public <R, T> R accept(@NonNull TypeVisitor<R, T> visitor, T obj) {
        return visitor.visitIntervalWeekType(this, obj);
    }

    @Override
    public int hashCode() {
        return CODE * 31;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof IntervalWeekType;
    }

    @Override
    public String toString() {
        return NAME;
    }
}
