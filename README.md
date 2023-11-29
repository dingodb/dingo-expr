# Dingo Expression

[![Build](https://github.com/dingodb/dingo-expr/actions/workflows/build.yml/badge.svg)](https://github.com/dingodb/dingo-expr/actions/workflows/build.yml)

Dingo Expression is an expression parsing and evaluating library, written in Java and C++.

## Getting Started

```java
// The original expression string.
String exprString = "(1 + 2) * (5 - (3 + 4))";
// parse it into an Expr object.
Expr expr = ExprParser.DEFAULT.parse(exprString);
// Compile the Expr to get a compiled Expr object.
Expr expr1 = ExprCompiler.SIMPLE.visit(expr);
// Evaluate it
Object result = expr1.eval();
```

Expr object can also be constructed by code, not only by parsing expression strings

```java
Expr expr = Exprs.op(Exprs.MUL, Exprs.op(Exprs.ADD, 1, 2), Exprs.op(Exprs.SUB, 5, Exprs.op(Exprs.ADD, 3, 4)));
```

There are also an "advanced" compiler, which can simplify expressions when compiling

```java
Expr expr1 = ExprCompiler.ADVANCED.visit(expr);
```

## Dependencies

For Maven POM

```xml

<dependencies>
    <!-- Required if you want to do expression parsing -->
    <dependency>
        <groupId>io.dingodb.expr</groupId>
        <artifactId>dingo-expr-parser</artifactId>
        <version>0.7.0</version>
    </dependency>

    <!-- Core module to compile and evaluate an expression -->
    <dependency>
        <groupId>io.dingodb.expr</groupId>
        <artifactId>dingo-expr-runtime</artifactId>
        <version>0.7.0</version>
    </dependency>
</dependencies>
```

For Gradle build

```groovy
dependencies {
    // Required if you want to do expression parsing
    implementation group: 'io.dingodb.expr', name: 'dingo-expr-runtime', version: '0.7.0'
    // Core module to compile and evaluate an expression
    implementation group: 'io.dingodb.expr', name: 'dingo-expr-parser', version: '0.7.0'
}
```

## Used By

- [DingoDB](https://github.com/dingodb/dingo)
