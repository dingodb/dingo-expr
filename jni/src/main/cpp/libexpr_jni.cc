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

#include <any>
#include <jni.h>

#include "runner.h"

using namespace dingodb::expr;

static jobject NewInteger(JNIEnv *jenv, const Wrap<int32_t> &v)
{
    if (v.has_value()) {
        jclass resClass = jenv->FindClass("Ljava/lang/Integer;");
        jmethodID ctor = jenv->GetMethodID(resClass, "<init>", "(I)V");
        return jenv->NewObject(resClass, ctor, v.value());
    }
    return nullptr;
}

static jobject NewLong(JNIEnv *jenv, const Wrap<int64_t> &v)
{
    if (v.has_value()) {
        jclass resClass = jenv->FindClass("Ljava/lang/Long;");
        jmethodID ctor = jenv->GetMethodID(resClass, "<init>", "(J)V");
        return jenv->NewObject(resClass, ctor, v.value());
    }
    return nullptr;
}

static jobject NewBoolean(JNIEnv *jenv, const Wrap<bool> &v)
{
    if (v.has_value()) {
        jclass resClass = jenv->FindClass("Ljava/lang/Boolean;");
        jmethodID ctor = jenv->GetMethodID(resClass, "<init>", "(Z)V");
        return jenv->NewObject(resClass, ctor, v.value());
    }
    return nullptr;
}

static jobject NewFloat(JNIEnv *jenv, const Wrap<float> &v)
{
    if (v.has_value()) {
        jclass resClass = jenv->FindClass("Ljava/lang/Float;");
        jmethodID ctor = jenv->GetMethodID(resClass, "<init>", "(F)V");
        return jenv->NewObject(resClass, ctor, v.value());
    }
    return nullptr;
}

static jobject NewDouble(JNIEnv *jenv, const Wrap<double> &v)
{
    if (v.has_value()) {
        jclass resClass = jenv->FindClass("Ljava/lang/Double;");
        jmethodID ctor = jenv->GetMethodID(resClass, "<init>", "(D)V");
        return jenv->NewObject(resClass, ctor, v.value());
    }
    return nullptr;
}

static jobject NewString(JNIEnv *jenv, const Wrap<String> &v)
{
    if (v.has_value()) {
        return jenv->NewStringUTF(v.value()->c_str());
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
    switch (type) {
    case TYPE_INT32:
        return NewInteger(jenv, runner->GetResult<int32_t>());
    case TYPE_INT64:
        return NewLong(jenv, runner->GetResult<int64_t>());
    case TYPE_BOOL:
        return NewBoolean(jenv, runner->GetResult<bool>());
    case TYPE_FLOAT:
        return NewFloat(jenv, runner->GetResult<float>());
    case TYPE_DOUBLE:
        return NewDouble(jenv, runner->GetResult<double>());
    case TYPE_STRING:
        return NewString(jenv, runner->GetResult<String>());
    }
    return nullptr;
}

JNIEXPORT void JNICALL Java_io_dingodb_expr_jni_LibExprJni_release(JNIEnv *jenv, jobject obj, jobject handle)
{
    auto runner = (dingodb::expr::Runner *)jenv->GetDirectBufferAddress(handle);
    delete runner;
}
