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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.ElementFilter;

public final class ProcessorUtils {
    private ProcessorUtils() {
    }

    public static @NonNull FieldSpec serialVersionUid() {
        return FieldSpec.builder(TypeName.LONG, "serialVersionUID")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$LL", new Random().nextLong())
            .build();
    }

    public static @Nullable ExecutableElement getMethodByNameAndParaTypes(
        @NonNull TypeElement element,
        String name,
        List<TypeName> paraTypes
    ) {
        List<ExecutableElement> methods = ElementFilter.methodsIn(element.getEnclosedElements());
        for (ExecutableElement m : methods) {
            if (m.getSimpleName().toString().equals(name)) {
                if (paraTypes == null) {
                    return m;
                }
                List<TypeName> types = m.getParameters().stream()
                    .map(Element::asType)
                    .map(TypeName::get)
                    .collect(Collectors.toList());
                if (types.equals(paraTypes)) {
                    return m;
                }
            }
        }
        return null;
    }

    public static MethodSpec.@NonNull Builder overridingWithAnnotations(ExecutableElement method) {
        MethodSpec.Builder builder = MethodSpec.overriding(method);
        List<? extends VariableElement> methodParas = method.getParameters();
        // javapoet does not copy annotations of parameter types, fix it.
        for (int i = 0; i < methodParas.size(); ++i) {
            ParameterSpec parameter = builder.parameters.get(i);
            builder.parameters.set(i, parameter.toBuilder().addAnnotations(
                methodParas.get(i).asType().getAnnotationMirrors().stream()
                    .map(AnnotationSpec::get)
                    .collect(Collectors.toList())
            ).build());
        }
        return builder;
    }
}
