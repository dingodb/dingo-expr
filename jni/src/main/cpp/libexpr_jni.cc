// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "libexpr_jni.h"

#include <jni.h>

#include "expr/runner.h"

using namespace dingodb::expr;

const struct JavaClassInfo {
    const char *name;
    const char *initSigature;
} JAVA_CLASS[] = {
    [TYPE_NULL] = {              nullptr, nullptr},
    [TYPE_INT32] = {"Ljava/lang/Integer;",  "(I)V"},
    [TYPE_INT64] = {   "Ljava/lang/Long;",  "(J)V"},
    [TYPE_BOOL] = {"Ljava/lang/Boolean;",  "(Z)V"},
    [TYPE_FLOAT] = {  "Ljava/lang/Float;",  "(F)V"},
    [TYPE_DOUBLE] = { "Ljava/lang/Double;",  "(D)V"},
    [TYPE_DECIMAL] = {              nullptr, nullptr},
    [TYPE_STRING] = {              nullptr, nullptr},
};

template <Byte T> jobject JavaObject(JNIEnv *jenv, const Operand &v)
{
    if (NotNull<TypeOf<T>>(v)) {
        jclass resClass = jenv->FindClass(JAVA_CLASS[T].name);
        jmethodID ctor = jenv->GetMethodID(resClass, "<init>", JAVA_CLASS[T].initSigature);
        return jenv->NewObject(resClass, ctor, GetValue<TypeOf<T>>(v));
    }
    return nullptr;
}

template <> jobject JavaObject<TYPE_STRING>(JNIEnv *jenv, const Operand &v)
{
    if (NotNull<String>(v)) {
        return jenv->NewStringUTF(GetValue<String>(v)->c_str());
    }
    return nullptr;
}

JNIEXPORT jobject JNICALL Java_io_dingodb_expr_jni_LibExprJni_decode(JNIEnv *jenv, jobject obj, jbyteArray exprBytes)
{
    jboolean isCopy;
    jsize len = jenv->GetArrayLength(exprBytes);
    jbyte *data = jenv->GetByteArrayElements(exprBytes, &isCopy);
    if (jenv->ExceptionCheck()) {
        return nullptr;
    }
    auto runner = new Runner();
    runner->Decode((Byte *)data, len);
    jenv->ReleaseByteArrayElements(exprBytes, data, JNI_ABORT);
    return jenv->NewDirectByteBuffer(runner, sizeof(Runner));
}

JNIEXPORT void JNICALL
Java_io_dingodb_expr_jni_LibExprJni_bindTuple(JNIEnv *jenv, jobject obj, jobject handle, jbyteArray tupleBytes)
{
}

JNIEXPORT jobject JNICALL Java_io_dingodb_expr_jni_LibExprJni_run(JNIEnv *jenv, jobject obj, jobject handle)
{
    auto runner = (Runner *)jenv->GetDirectBufferAddress(handle);
    runner->Run();
    auto type = runner->GetType();
    Operand v = runner->Get();
    switch (type) {
    case TYPE_INT32:
        return JavaObject<TYPE_INT32>(jenv, v);
    case TYPE_INT64:
        return JavaObject<TYPE_INT64>(jenv, v);
    case TYPE_BOOL:
        return JavaObject<TYPE_BOOL>(jenv, v);
    case TYPE_FLOAT:
        return JavaObject<TYPE_FLOAT>(jenv, v);
    case TYPE_DOUBLE:
        return JavaObject<TYPE_DOUBLE>(jenv, v);
    case TYPE_STRING:
        return JavaObject<TYPE_STRING>(jenv, v);
    }
    return nullptr;
}

JNIEXPORT void JNICALL Java_io_dingodb_expr_jni_LibExprJni_release(JNIEnv *jenv, jobject obj, jobject handle)
{
    auto runner = (dingodb::expr::Runner *)jenv->GetDirectBufferAddress(handle);
    delete runner;
}
