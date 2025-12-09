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

public enum DateTimeType {

    DATE(false, "DATE"),
    TIME(false, "TIME"),
    TIMESTAMP(false, "TIMESTAMP"),

    TIMESTAMP_TZ(true, "TIMESTAMP WITH TIME ZONE");

    private final boolean timeZoneSensitive;
    private final String sqlName;

    DateTimeType(boolean timeZoneSensitive, String sqlName) {
        this.timeZoneSensitive = timeZoneSensitive;
        this.sqlName = sqlName;
    }

    public boolean isTimeZoneSensitive() {
        return timeZoneSensitive;
    }

    public String getSqlName() {
        return sqlName;
    }

    public static DateTimeType fromString(String type) {
        if (type == null) {
            return TIMESTAMP;
        }

        switch (type.toUpperCase()) {
            case "DATE":
                return DATE;
            case "TIME":
                return TIME;
            case "TIMESTAMP":
                return TIMESTAMP;
            case "TIMESTAMP_TZ":
                return TIMESTAMP_TZ;
            default:
                return TIMESTAMP;
        }
    }
}
