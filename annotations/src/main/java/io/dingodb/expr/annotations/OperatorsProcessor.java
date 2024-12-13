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

import com.google.auto.service.AutoService;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalYearType;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;

@AutoService(Processor.class)
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class OperatorsProcessor extends AbstractProcessor {
    private static final String KEY_OF_METHOD_NAME = "keyOf";
    private static final String EVAL_VALUE_METHOD_NAME = "evalValue";
    private static final String EVAL_NON_NULL_VALUE_METHOD_NAME = "evalNonNullValue";
    private static final String GET_TYPE_METHOD_NAME = "getType";
    private static final String GET_OP_METHOD_NAME = "getOp";
    private static final String GET_KEY_METHOD_NAME = "getKey";
    private static final String OP_MAP_VAR_NAME = "opMap";
    private static final String INSTANCE_VAR_NAME = "INSTANCE";

    private static final String TYPES_CLASS_NAME = "io.dingodb.expr.common.type.Types";
    private static final String EXPR_CONFIG_CLASS_NAME = "io.dingodb.expr.runtime.ExprConfig";

    private static final Pattern NAME_PATTERN = Pattern.compile("^([a-zA-Z]+)\\d*$");

    private static @NonNull CodeBlock codeEvalParas(
        String methodName,
        List<CodeBlock> evalMethodParas,
        @NonNull List<? extends VariableElement> paras,
        VariableElement paraConfig
    ) {
        CodeBlock.Builder builder = CodeBlock.builder();
        builder.add("return $L(", methodName);
        List<CodeBlock> paraBlocks = new ArrayList<>(paras.size());
        for (int i = 0; i < paras.size(); ++i) {
            CodeBlock.Builder b = CodeBlock.builder();
            b.add("($T) ", TypeName.get(paras.get(i).asType()).box());
            b.add(evalMethodParas.get(i));
            paraBlocks.add(b.build());
        }
        if (paraConfig != null) {
            paraBlocks.add(evalMethodParas.get(evalMethodParas.size() - 1));
        }
        builder.add(CodeBlock.join(paraBlocks, ", "));
        builder.add(");\n");
        return builder.build();
    }

    private static @NonNull CodeBlock codeKeyOf(
        @NonNull TypeElement typesClass,
        @NonNull List<? extends VariableElement> paras
    ) {
        CodeBlock.Builder builder = CodeBlock.builder().add(KEY_OF_METHOD_NAME + "(");
        List<CodeBlock> paraBlocks = new ArrayList<>(paras.size());
        for (VariableElement para : paras) {
            CodeBlock.Builder b = CodeBlock.builder();
            b.add(typeOf(typesClass, TypeName.get(para.asType())));
            paraBlocks.add(b.build());
        }
        builder.add(CodeBlock.join(paraBlocks, ", "));
        builder.add(")");
        return builder.build();
    }

    private static @NonNull CodeBlock typeOf(TypeElement typesClass, TypeName typeName) {
        CodeBlock.Builder builder = CodeBlock.builder();
        builder.add("$T.$L", typesClass, getType(typeName));
        return builder.build();
    }

    /**
     * Get the type field name in class {@code Types} corresponding the specified {@link TypeName}.
     *
     * @param typeName the {@link TypeName}
     * @return the type field name in class {@code Types}
     */
    private static String getType(@NonNull TypeName typeName) {
        if (typeName instanceof ParameterizedTypeName) {
            typeName = ((ParameterizedTypeName) typeName).rawType;
        }
        if (typeName.equals(TypeName.INT) || typeName.equals(TypeName.INT.box())) {
            return "INT";
        } else if (typeName.equals(TypeName.LONG) || typeName.equals(TypeName.LONG.box())) {
            return "LONG";
        } else if (typeName.equals(TypeName.BOOLEAN) || typeName.equals(TypeName.BOOLEAN.box())) {
            return "BOOL";
        } else if (typeName.equals(TypeName.FLOAT) || typeName.equals(TypeName.FLOAT.box())) {
            return "FLOAT";
        } else if (typeName.equals(TypeName.DOUBLE) || typeName.equals(TypeName.DOUBLE.box())) {
            return "DOUBLE";
        } else if (typeName.equals(TypeName.get(BigDecimal.class))) {
            return "DECIMAL";
        } else if (typeName.equals(TypeName.get(String.class))) {
            return "STRING";
        } else if (typeName instanceof ArrayTypeName
                   && ((ArrayTypeName) typeName).componentType.equals(TypeName.BYTE)
        ) {
            return "BYTES";
        } else if (typeName.equals(TypeName.get(Date.class))) {
            return "DATE";
        } else if (typeName.equals(TypeName.get(Time.class))) {
            return "TIME";
        } else if (typeName.equals(TypeName.get(Timestamp.class))) {
            return "TIMESTAMP";
        } else if (typeName.equals(TypeName.get(Void.class))) {
            return "NULL";
        } else if (typeName.equals(TypeName.get(IntervalYearType.IntervalYear.class))) {
            return "YEAR";
        } else if (typeName.equals(TypeName.get(IntervalMonthType.IntervalMonth.class))) {
            return "MONTH";
        } else if (typeName.equals(TypeName.get(IntervalDayType.IntervalDay.class))) {
            return "DAY";
        }
        return "ANY";
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(Operators.class.getName());
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (annotations == null) {
            return true;
        }
        for (TypeElement annotation : annotations) {
            if (annotation.getQualifiedName().contentEquals(Operators.class.getCanonicalName())) {
                try {
                    Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(annotation);
                    for (Element element : elements) {
                        if (!(element instanceof TypeElement)) {
                            throw showError(
                                "Element annotated with \""
                                + Operators.class.getSimpleName() + "\" must be a class."
                            );
                        }
                        OperatorsInfo info = prepareInfo((TypeElement) element);
                        List<ExecutableElement> methods =
                            ElementFilter.methodsIn(element.getEnclosedElements()).stream()
                                .filter(m -> m.getModifiers().contains(Modifier.STATIC))
                                .filter(m -> !m.getSimpleName().toString().equals(KEY_OF_METHOD_NAME))
                                .collect(Collectors.toList());
                        for (ExecutableElement method : methods) {
                            generateOperator(method, info);
                        }
                        generateOperatorFactory(info);
                    }
                } catch (IOException e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getLocalizedMessage());
                }
            }
        }
        return true;
    }

    private @NonNull OperatorsInfo prepareInfo(@NonNull TypeElement element) {
        Element pkg = element.getEnclosingElement();
        if (pkg.getKind() != ElementKind.PACKAGE) {
            throw showError(
                "Class annotated with \""
                + Operators.class.getCanonicalName() + "\" must not be an inner class."
            );
        }
        final String packageName = pkg.asType().toString();
        TypeElement typesClass = processingEnv.getElementUtils().getTypeElement(TYPES_CLASS_NAME);
        TypeElement exprConfigClass = processingEnv.getElementUtils().getTypeElement(EXPR_CONFIG_CLASS_NAME);
        if (typesClass == null || exprConfigClass == null) {
            throw showError(
                "Cannot find class \"" + TYPES_CLASS_NAME
                + "\" or class \"" + EXPR_CONFIG_CLASS_NAME + "\"."
            );
        }
        boolean nullable = element.getAnnotation(Operators.class).nullable();
        String evalMethodName = nullable ? EVAL_VALUE_METHOD_NAME : EVAL_NON_NULL_VALUE_METHOD_NAME;
        ExecutableElement evalMethod = getOverridingMethod(element, evalMethodName, null);
        if (evalMethod == null) {
            throw showError(
                "Cannot find method \"" + evalMethodName
                + "\" in base class \"" + element.getQualifiedName() + "\"."
            );
        }
        List<? extends VariableElement> evalMethodParameters = evalMethod.getParameters();
        boolean variadic = (evalMethodParameters.size() == 2
                            && evalMethodParameters.get(0).asType().getKind() == TypeKind.ARRAY);
        return new OperatorsInfo(
            packageName,
            typesClass,
            exprConfigClass,
            element,
            evalMethod,
            variadic
        );
    }

    private void generateOperator(
        final @NonNull ExecutableElement element,
        final @NonNull OperatorsInfo info
    ) {
        final List<? extends VariableElement> paras = new ArrayList<>(element.getParameters());
        VariableElement paraConfig = null;
        if (processingEnv.getTypeUtils().isSubtype(
            paras.get(paras.size() - 1).asType(),
            info.getExprConfigClass().asType()
        )) {
            paraConfig = paras.get(paras.size() - 1);
            paras.remove(paras.size() - 1);
        }

        // Generate class.
        final String methodName = element.getSimpleName().toString();
        Matcher matcher = NAME_PATTERN.matcher(methodName);
        if (!matcher.find()) {
            throw showError("Not a valid method name: \"" + methodName + "\".");
        }
        StringBuilder b = new StringBuilder();
        for (VariableElement para : paras) {
            b.append(StringUtils.capitalize(getType(TypeName.get(para.asType())).toLowerCase()));
        }
        final String signature = b.toString();
        final String className = StringUtils.capitalize(matcher.group(1)) + signature;
        TypeSpec.Builder builder = TypeSpec.classBuilder(className)
            .superclass(info.getBase().asType())
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .addField(ProcessorUtils.serialVersionUid());

        // Generate eval method.
        final ExecutableElement evalMethod = info.getEvalMethod();
        final List<? extends VariableElement> evalMethodParameters = evalMethod.getParameters();
        List<CodeBlock> evalMethodParasCode;
        if (info.isVariadic()) {
            String paraName = evalMethodParameters.get(0).getSimpleName().toString();
            evalMethodParasCode = new ArrayList<>(paras.size());
            for (int i = 0; i < paras.size(); ++i) {
                evalMethodParasCode.add(CodeBlock.of("$L[$L]", paraName, i));
            }
            evalMethodParasCode.add(CodeBlock.of(evalMethodParameters.get(1).getSimpleName().toString()));
        } else {
            int evalParaCount = evalMethodParameters.size();
            if (paras.size() + 1 != evalParaCount) {
                throw showError(
                    "Required number of eval method parameters for \""
                    + info.getBase().getQualifiedName() + "\" is " + (paras.size() + 1)
                    + ", but is " + evalParaCount + ", maybe the base is wrong."
                );
            }
            evalMethodParasCode = evalMethod.getParameters().stream()
                .map(VariableElement::getSimpleName)
                .map(Object::toString)
                .map(s -> CodeBlock.of("$L", s))
                .collect(Collectors.toList());
        }
        final TypeName returnType = TypeName.get(element.getReturnType()).box();
        builder.addMethod(
            ProcessorUtils.overridingWithAnnotations(evalMethod)
                .returns(returnType)
                .addCode(codeEvalParas(methodName, evalMethodParasCode, paras, paraConfig))
                .build()
        );

        // Generate getType method.
        ExecutableElement getTypeMethod = getOverridingMethod(
            info.getBase(),
            GET_TYPE_METHOD_NAME,
            Collections.emptyList()
        );
        if (getTypeMethod != null) {
            builder.addMethod(
                ProcessorUtils.overridingWithAnnotations(getTypeMethod)
                    .addStatement("return $T.$L", info.getTypesClass(), getType(returnType))
                    .build()
            );
        }

        // Generate getKey method.
        final CodeBlock keyOf = codeKeyOf(info.getTypesClass(), paras);
        ExecutableElement getKeyMethod = getOverridingMethod(
            info.getBase(),
            GET_KEY_METHOD_NAME,
            Collections.emptyList()
        );
        if (getKeyMethod != null) {
            builder.addMethod(
                ProcessorUtils.overridingWithAnnotations(getKeyMethod)
                    .addStatement("return $L", keyOf)
                    .build()
            );
        }

        // Save to operator map.
        info.getOperatorMap().put(
            signature,
            new OperatorInfo(className, returnType, keyOf, builder.build())
        );
    }

    private void generateOperatorFactory(@NonNull OperatorsInfo info) throws IOException {
        String packageName = info.getPackageName();
        final Map<String, OperatorInfo> operatorMap = info.getOperatorMap();
        if (operatorMap.isEmpty()) {
            return;
        }
        CodeBlock.Builder initBuilder = CodeBlock.builder();
        for (Map.Entry<String, OperatorInfo> entry : operatorMap.entrySet()) {
            OperatorInfo operatorInfo = entry.getValue();
            initBuilder.addStatement("$L.put($L, new $N())",
                OP_MAP_VAR_NAME,
                entry.getValue().getKeyOf(),
                operatorInfo.getInnerClass()
            );
        }
        ClassName className = ClassName.get(
            packageName,
            info.getBase().getSimpleName().toString() + "Factory"
        );
        ExecutableElement getOpMethod = getOverridingMethod(
            info.getBase(),
            GET_OP_METHOD_NAME,
            null
        );
        if (getOpMethod == null) {
            throw showError(
                "Cannot find method \"" + GET_OP_METHOD_NAME
                + "\" in base class \"" + info.getBase().getQualifiedName() + "\"."
            );
        }
        TypeSpec.Builder builder = TypeSpec.classBuilder(className)
            .superclass(info.getBase().asType())
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addField(ProcessorUtils.serialVersionUid())
            .addField(FieldSpec.builder(className, INSTANCE_VAR_NAME)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer("new $T()", className)
                .build())
            .addField(FieldSpec.builder(
                    ParameterizedTypeName.get(
                        ClassName.get(Map.class),
                        TypeName.OBJECT,
                        TypeName.get(info.getBase().asType())
                    ),
                    OP_MAP_VAR_NAME
                )
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .initializer("new $T<>()", HashMap.class)
                .build())
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE)
                .addStatement("super()")
                .addCode(initBuilder.build())
                .build())
            .addMethod(ProcessorUtils.overridingWithAnnotations(getOpMethod)
                .addStatement("return $L.get($L)", OP_MAP_VAR_NAME, getOpMethod.getParameters().get(0))
                .build());
        for (Map.Entry<String, OperatorInfo> entry : operatorMap.entrySet()) {
            builder.addType(entry.getValue().getInnerClass());
        }
        TypeSpec typeSpec = builder.build();
        saveSourceFile(packageName, typeSpec);
    }

    private @NonNull RuntimeException showError(String message) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message);
        return new IllegalStateException(message);
    }

    private List<TypeElement> findSuperTypes(@NonNull TypeElement element) {
        return processingEnv.getTypeUtils().directSupertypes(element.asType()).stream()
            .filter(i -> i.getKind() == TypeKind.DECLARED)
            .map(t -> ((DeclaredType) t).asElement())
            .map(e -> (TypeElement) e)
            .collect(Collectors.toList());
    }

    private @Nullable ExecutableElement getOverridingMethod(
        @NonNull TypeElement element,
        String name,
        List<TypeName> paraTypes
    ) {
        ExecutableElement method = ProcessorUtils.getMethodByNameAndParaTypes(element, name, paraTypes);
        if (method != null) {
            Set<Modifier> modifiers = method.getModifiers();
            if (!modifiers.contains(Modifier.FINAL) && !modifiers.contains(Modifier.STATIC)) {
                return method;
            }
            return null;
        }
        for (TypeElement e : findSuperTypes(element)) {
            method = getOverridingMethod(e, name, paraTypes);
            if (method != null) {
                return method;
            }
        }
        return null;
    }

    /**
     * Save a source file.
     *
     * @param packageName the package name
     * @param typeSpec    the TypeSpec of the class/interface
     */
    private void saveSourceFile(String packageName, TypeSpec typeSpec) throws IOException {
        JavaFile javaFile = JavaFile.builder(packageName, typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
        javaFile.writeTo(processingEnv.getFiler());
    }
}
