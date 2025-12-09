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

package io.dingodb.expr.common.timezone.core;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Objects;

public abstract class DingoDateTime {
    protected final DateTimeType type;
    protected final boolean timeZoneSensitive;

    protected DingoDateTime(DateTimeType type) {
        this.type = type;
        this.timeZoneSensitive = type.isTimeZoneSensitive();
    }

    public DateTimeType getType() {
        return type;
    }

    public boolean isTimeZoneSensitive() {
        return timeZoneSensitive;
    }

    public abstract Object getValue();

    public static class DingoLocalDate extends DingoDateTime {
        private final LocalDate value;

        public DingoLocalDate(LocalDate value) {
            super(DateTimeType.DATE);
            this.value = Objects.requireNonNull(value);
        }

        @Override
        public LocalDate getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "DingoLocalDate{value=" + value + "}";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof DingoLocalDate)) {
                return false;
            }
            DingoLocalDate that = (DingoLocalDate) obj;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    public static class DingoLocalTime extends DingoDateTime {
        private final LocalTime value;

        public DingoLocalTime(LocalTime value) {
            super(DateTimeType.TIME);
            this.value = Objects.requireNonNull(value);
        }

        @Override
        public LocalTime getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "DingoLocalTime{value=" + value + "}";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof DingoLocalTime)) {
                return false;
            }
            DingoLocalTime that = (DingoLocalTime) obj;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    public static class DingoLocalDateTime extends DingoDateTime {
        private final LocalDateTime value;

        public DingoLocalDateTime(LocalDateTime value) {
            super(DateTimeType.TIMESTAMP);
            this.value = Objects.requireNonNull(value);
        }

        @Override
        public LocalDateTime getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "DingoLocalDateTime{value=" + value + "}";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof DingoLocalDateTime)) {
                return false;
            }
            DingoLocalDateTime that = (DingoLocalDateTime) obj;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    public static class DingoTimestampTZ extends DingoDateTime {
        private final Instant utcValue;
        private final ZoneId originalZone;

        public DingoTimestampTZ(Instant utcValue, ZoneId originalZone) {
            super(DateTimeType.TIMESTAMP_TZ);
            this.utcValue = Objects.requireNonNull(utcValue);
            this.originalZone = Objects.requireNonNull(originalZone);
        }

        public Instant getUtcValue() {
            return utcValue;
        }

        public ZoneId getOriginalZone() {
            return originalZone;
        }

        @Override
        public Instant getValue() {
            return utcValue;
        }

        @Override
        public String toString() {
            return "DingoTimestampTZ{utcValue=" + utcValue + ", originalZone=" + originalZone + "}";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof DingoTimestampTZ)) {
                return false;
            }
            DingoTimestampTZ that = (DingoTimestampTZ) obj;
            return Objects.equals(utcValue, that.utcValue)
                   && Objects.equals(originalZone, that.originalZone);
        }

        @Override
        public int hashCode() {
            return Objects.hash(utcValue, originalZone);
        }
    }
}
