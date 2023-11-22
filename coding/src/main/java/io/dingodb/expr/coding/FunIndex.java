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

import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.mathematical.AcosFunFactory;
import io.dingodb.expr.runtime.op.mathematical.AsinFunFactory;
import io.dingodb.expr.runtime.op.mathematical.AtanFunFactory;
import io.dingodb.expr.runtime.op.mathematical.CeilFunFactory;
import io.dingodb.expr.runtime.op.mathematical.CosFunFactory;
import io.dingodb.expr.runtime.op.mathematical.CoshFunFactory;
import io.dingodb.expr.runtime.op.mathematical.ExpFunFactory;
import io.dingodb.expr.runtime.op.mathematical.FloorFunFactory;
import io.dingodb.expr.runtime.op.mathematical.LogFunFactory;
import io.dingodb.expr.runtime.op.mathematical.SinFunFactory;
import io.dingodb.expr.runtime.op.mathematical.SinhFunFactory;
import io.dingodb.expr.runtime.op.mathematical.TanFunFactory;
import io.dingodb.expr.runtime.op.mathematical.TanhFunFactory;
import io.dingodb.expr.runtime.op.string.ConcatFunFactory;
import io.dingodb.expr.runtime.op.string.LTrim1FunFactory;
import io.dingodb.expr.runtime.op.string.LeftFunFactory;
import io.dingodb.expr.runtime.op.string.LowerFunFactory;
import io.dingodb.expr.runtime.op.string.Mid2FunFactory;
import io.dingodb.expr.runtime.op.string.Mid3FunFactory;
import io.dingodb.expr.runtime.op.string.RTrim1FunFactory;
import io.dingodb.expr.runtime.op.string.RightFunFactory;
import io.dingodb.expr.runtime.op.string.Substr2FunFactory;
import io.dingodb.expr.runtime.op.string.Substr3FunFactory;
import io.dingodb.expr.runtime.op.string.Trim1FunFactory;
import io.dingodb.expr.runtime.op.string.UpperFunFactory;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

final class FunIndex {
    private FunIndex() {
    }

    static int getUnary(@NonNull UnaryOp op) {
        String funName = op.getName();
        Object key = op.getKey();
        boolean isIntegral = key.equals(Types.INT) || key.equals(Types.LONG);
        int funIndex = -1;
        switch (funName) {
            case CeilFunFactory.NAME:
                if (key.equals(Types.DOUBLE)) {
                    funIndex = 0x01;
                } else if (isIntegral) {
                    funIndex = 0;
                }
                break;
            case FloorFunFactory.NAME:
                if (key.equals(Types.DOUBLE)) {
                    funIndex = 0x02;
                } else if (isIntegral) {
                    funIndex = 0;
                }
                break;
            case SinFunFactory.NAME:
                funIndex = 0x07;
                break;
            case CosFunFactory.NAME:
                funIndex = 0x08;
                break;
            case TanFunFactory.NAME:
                funIndex = 0x09;
                break;
            case AsinFunFactory.NAME:
                funIndex = 0x0A;
                break;
            case AcosFunFactory.NAME:
                funIndex = 0x0B;
                break;
            case AtanFunFactory.NAME:
                funIndex = 0x0C;
                break;
            case SinhFunFactory.NAME:
                funIndex = 0x0D;
                break;
            case CoshFunFactory.NAME:
                funIndex = 0x0E;
                break;
            case TanhFunFactory.NAME:
                funIndex = 0x0F;
                break;
            case ExpFunFactory.NAME:
                funIndex = 0x10;
                break;
            case LogFunFactory.NAME:
                funIndex = 0x11;
                break;
            case LowerFunFactory.NAME:
                funIndex = 0x22;
                break;
            case UpperFunFactory.NAME:
                funIndex = 0x23;
                break;
            case Trim1FunFactory.NAME:
                funIndex = 0x26;
                break;
            case LTrim1FunFactory.NAME:
                funIndex = 0x28;
                break;
            case RTrim1FunFactory.NAME:
                funIndex = 0x2A;
                break;
            default:
                break;
        }
        return funIndex;
    }

    static int getBinary(@NonNull BinaryOp op) {
        String funName = op.getName();
        int funIndex = -1;
        switch (funName) {
            case ConcatFunFactory.NAME:
                funIndex = 0x21;
                break;
            case LeftFunFactory.NAME:
                funIndex = 0x24;
                break;
            case RightFunFactory.NAME:
                funIndex = 0x25;
                break;
            case Substr2FunFactory.NAME:
                funIndex = 0x2D;
                break;
            case Mid2FunFactory.NAME:
                funIndex = 0x2F;
                break;
            default:
                break;
        }
        return funIndex;
    }

    static int getTertiary(@NonNull TertiaryOp op) {
        String funName = op.getName();
        int funIndex = -1;
        switch (funName) {
            case Substr3FunFactory.NAME:
                funIndex = 0x2C;
                break;
            case Mid3FunFactory.NAME:
                funIndex = 0x2E;
                break;
            default:
                break;
        }
        return funIndex;
    }
}
