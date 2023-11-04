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

package io.dingodb.expr.annotations;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

@RequiredArgsConstructor
public class OperatorsInfo {
    @Getter
    private final String packageName;
    @Getter
    private final TypeElement typesClass;
    @Getter
    private final TypeElement exprConfigClass;
    @Getter
    private final TypeElement base;
    @Getter
    private final ExecutableElement evalMethod;
    @Getter
    private final boolean variadic;
    @Getter
    private final Map<String, OperatorInfo> operatorMap = new HashMap<>();
}
