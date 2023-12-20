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

import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpStringBuilder;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.expr.runtime.utils.CodecUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRelOpCoder {
    @Test
    public void test() throws ExprParseException {
        RelOp op = RelOpStringBuilder.builder(RelConfig.DEFAULT)
            .filter("$[2] > 50")
            .build();
        op.init(Types.tuple("INT", "STRING", "FLOAT"), RelConfig.DEFAULT);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        RelOpCoder.INSTANCE.visit(op, os);
        assertThat(os.toByteArray()).isEqualTo(CodecUtils.hexStringToBytes("7134021442480000930400"));
    }
}
