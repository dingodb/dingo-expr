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

package io.dingodb.expr.console;

import io.dingodb.expr.parser.ExprParser;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ResourceBundle;

@Slf4j
public final class DingoExprConsole {
    private DingoExprConsole() {
    }

    public static void main(String[] args) {
        ResourceBundle config = ResourceBundle.getBundle("config");
        System.out.println(config.getString("hello"));
        String prompt = config.getString("prompt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print(prompt + ' ');
            try {
                String input = reader.readLine();
                if (input == null || input.isEmpty()) {
                    break;
                }
                Expr expr = ExprParser.DEFAULT.parse(input);
                Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
                System.out.println(expr1.eval());
            } catch (Exception e) {
                log.error(e.toString());
            }
        }
    }
}
