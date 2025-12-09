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

package io.dingodb.expr.common.timezone.operations;

public class OperationResult {
    private final Object value;
    private final Class<?> valueType;
    private final boolean success;
    private final String errorMessage;

    private OperationResult(Object value, Class<?> valueType, boolean success, String errorMessage) {
        this.value = value;
        this.valueType = valueType;
        this.success = success;
        this.errorMessage = errorMessage;
    }

    public static OperationResult success(Object value) {
        return new OperationResult(value, value != null ? value.getClass() : null, true, null);
    }

    public static OperationResult success(Object value, Class<?> valueType) {
        return new OperationResult(value, valueType, true, null);
    }

    public static OperationResult failure(String errorMessage) {
        return new OperationResult(null, null, false, errorMessage);
    }

    // Getters
    public Object getValue() {
        return value;
    }

    public Class<?> getValueType() {
        return valueType;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @SuppressWarnings("unchecked")
    public <T> T getValueAs(Class<T> type) {
        if (value != null && type.isInstance(value)) {
            return (T) value;
        }
        return null;
    }
}
