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

package io.dingodb.expr.coding;

import io.dingodb.expr.common.type.ArrayType;
import io.dingodb.expr.common.type.BoolType;
import io.dingodb.expr.common.type.DateType;
import io.dingodb.expr.common.type.DecimalType;
import io.dingodb.expr.common.type.DoubleType;
import io.dingodb.expr.common.type.FloatType;
import io.dingodb.expr.common.type.IntType;
import io.dingodb.expr.common.type.LongType;
import io.dingodb.expr.common.type.StringType;
import io.dingodb.expr.common.type.TimestampType;
import io.dingodb.expr.common.type.TypeVisitorBase;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.utils.CodecUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class ValCoder extends TypeVisitorBase<CodingFlag, OutputStream> {
    private static final byte NULL = (byte) 0x00;
    private static final byte CONST = (byte) 0x10;
    private static final byte CONST_N = (byte) 0x20;

    private final Val val;

    @SneakyThrows
    @Override
    public CodingFlag visitIntType(@NonNull IntType type, OutputStream obj) {
        Integer value = (Integer) val.getValue();
        if (value != null) {
            if (value >= 0) {
                obj.write(CONST | TypeCoder.TYPE_INT32);
                CodecUtils.encodeVarInt(obj, value);
            } else {
                obj.write(CONST_N | TypeCoder.TYPE_INT32);
                CodecUtils.encodeVarInt(obj, -(long) value); // value may overflow for `Integer.MIN_VALUE`.
            }
        } else {
            obj.write(NULL | TypeCoder.TYPE_INT32);
        }
        return CodingFlag.OK;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitLongType(@NonNull LongType type, OutputStream obj) {
        Long value = (Long) val.getValue();
        if (value != null) {
            if (value >= 0 || value == Long.MIN_VALUE) {
                obj.write(CONST | TypeCoder.TYPE_INT64);
                CodecUtils.encodeVarInt(obj, value);
            } else {
                obj.write(CONST_N | TypeCoder.TYPE_INT64);
                CodecUtils.encodeVarInt(obj, -value);
            }
        } else {
            obj.write(NULL | TypeCoder.TYPE_INT64);
        }
        return CodingFlag.OK;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitDateType(@NonNull DateType type, OutputStream obj) {
        Date value = (Date) val.getValue();
        if (value != null) {
            long milliseconds = value.getTime();
            obj.write(CONST | TypeCoder.TYPE_DATE);
            CodecUtils.encodeVarInt(obj, milliseconds);
        } else {
            obj.write(NULL | TypeCoder.TYPE_DATE);
        }
        return CodingFlag.OK;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitTimestampType(@NonNull TimestampType type, OutputStream obj) {
        Timestamp value = (Timestamp) val.getValue();
        if (value != null) {
            long milliseconds = value.getTime();
            obj.write(CONST | TypeCoder.TYPE_TIMESTAMP);
            CodecUtils.encodeVarInt(obj, milliseconds);
        } else {
            obj.write(NULL | TypeCoder.TYPE_TIMESTAMP);
        }
        return CodingFlag.OK;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitFloatType(@NonNull FloatType type, OutputStream obj) {
        Float value = (Float) val.getValue();
        if (value != null) {
            obj.write(CONST | TypeCoder.TYPE_FLOAT);
            CodecUtils.encodeFloat(obj, value);
        } else {
            obj.write(NULL | TypeCoder.TYPE_FLOAT);
        }
        return CodingFlag.OK;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitDoubleType(@NonNull DoubleType type, OutputStream obj) {
        Double value = (Double) val.getValue();
        if (value != null) {
            obj.write(CONST | TypeCoder.TYPE_DOUBLE);
            CodecUtils.encodeDouble(obj, value);
        } else {
            obj.write(NULL | TypeCoder.TYPE_DOUBLE);
        }
        return CodingFlag.OK;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitBoolType(@NonNull BoolType type, OutputStream obj) {
        Boolean value = (Boolean) val.getValue();
        if (value != null) {
            obj.write(value ? CONST | TypeCoder.TYPE_BOOL : CONST_N | TypeCoder.TYPE_BOOL);
        } else {
            obj.write(NULL | TypeCoder.TYPE_BOOL);
        }
        return CodingFlag.OK;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitStringType(@NonNull StringType type, OutputStream obj) {
        String value = (String) val.getValue();
        if (value != null) {
            obj.write(CONST | TypeCoder.TYPE_STRING);
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            CodecUtils.encodeVarInt(obj, bytes.length);
            obj.write(bytes);
        } else {
            obj.write(NULL | TypeCoder.TYPE_STRING);
        }
        return CodingFlag.OK;
    }

    @SneakyThrows
    @Override
    public CodingFlag visitDecimalType(@NonNull DecimalType type, OutputStream obj) {
        BigDecimal value = (BigDecimal) val.getValue();
        if (value != null) {
            obj.write(CONST | TypeCoder.TYPE_DECIMAL);
            byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
            CodecUtils.encodeVarInt(obj, bytes.length);
            obj.write(bytes);
        } else {
            obj.write(NULL | TypeCoder.TYPE_DECIMAL);
        }
        return CodingFlag.OK;
    }

    @Override
    public CodingFlag visitArrayType(@NonNull ArrayType type, OutputStream obj) {
        return new ArrayCoder(val.getValue()).visit(type.getElementType(), obj);
    }
}
