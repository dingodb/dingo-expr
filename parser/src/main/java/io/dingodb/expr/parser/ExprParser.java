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

package io.dingodb.expr.parser;

import io.dingodb.expr.parser.antlr.DingoExprErrorListener;
import io.dingodb.expr.parser.antlr.DingoExprParserVisitorImpl;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.parser.exception.ExprSyntaxError;
import io.dingodb.expr.runtime.expr.Expr;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;

public final class ExprParser {
    public static final ExprParser DEFAULT = new ExprParser(new DefaultFunFactory());

    private final DingoExprParserVisitorImpl visitor;

    public ExprParser(FunFactory funFactory) {
        visitor = new DingoExprParserVisitorImpl(funFactory);
    }

    /**
     * Parse a given string input into an un-compiled {@link Expr}.
     *
     * @param input the given string
     * @return the {@link Expr}
     * @throws ExprParseException if errors occurred in parsing
     */
    public Expr parse(String input) throws ExprParseException {
        CharStream stream = CharStreams.fromString(input);
        DingoExprLexer lexer = new DingoExprLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        DingoExprParser parser = new DingoExprParser(tokens);
        parser.removeErrorListeners();
        DingoExprErrorListener errorListener = new DingoExprErrorListener();
        parser.addErrorListener(errorListener);
        ParseTree tree = parser.expr();
        List<String> errorMessages = errorListener.getErrorMessages();
        if (!errorMessages.isEmpty()) {
            throw new ExprSyntaxError(errorMessages);
        }
        try {
            return visitor.visit(tree);
        } catch (ParseCancellationException e) {
            throw new ExprParseException(e);
        }
    }

    public FunFactory getFunFactory() {
        return visitor.getFunFactory();
    }
}
