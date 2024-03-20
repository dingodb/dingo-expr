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

## Variables

Variables are allowed in expressions. In order to compile an expression with variables, an implementation of
interface `CompileContext` must be provided, which tells the compiler the type and runtime identification of all the
existing variables

```java
Expr expr1 = ExprCompiler.SIMPLE.visit(expr, compileContext);
```

To evaluate the expression, an implementation of interface `EvalContext` must be provided to get the real value of
variables

```java
Object result = expr1.eval(evalContext, config);
```

where `config` is an object implementing `ExprConfig` which can be `ExprConfig.SIMPLE`, `ExprConfig.ADVANCED` or any
customized implementations.

## Functions

Functions are all pre-defined and there is no syntax to define a function in expressions, but you can implement a
customized function and register it to the parsing, compiling and evaluating system.

All this begins with an interface `FunFactory`

```java
public interface FunFactory {
    // Register a function with no parameters
    void registerNullaryFun(@NonNull String funName, @NonNull NullaryOp op);

    // Register a function with one parameter
    void registerUnaryFun(@NonNull String funName, @NonNull UnaryOp op);

    // Register a function with two parameters
    void registerBinaryFun(@NonNull String funName, @NonNull BinaryOp op);

    // Register a function with three parameters
    void registerTertiaryFun(@NonNull String funName, @NonNull TertiaryOp op);

    // Register a function with varidic parameters
    void registerVariadicFun(@NonNull String funName, @NonNull VariadicOp op);
}
```

An implementation of `FunFactory` is then used in a `ExprParser`

```java
ExprParser parser = new ExprParser(new DefaultFunFactory(ExprConfig.SIMPLE));
```

where the `DefaultFunFactory` is a provided implementation of `FunFactory` which contains the pre-defined functions.
Actually there is a `ExprParser.DEFAULT` defined as above, which can be used. In this case, you can register a function
by `ExprParser.DEFAULT.getFunFactory().registerUnaryFun(funName, UnaryOp op)`.

Note, in `DefaultFunFactory`, the five kinds of functions are stored in separate maps, so different kinds of functions
with the same name is allowed.

After register the function, the `ExprParser` can recognize the new function and convert to the corresponding `Op`.
Then, the only thing left is to implement an `Op`.

For example, if you want to register an `UnaryOp`, which has only one parameter, by calling `registerUnaryFun`, you
should extend the class `UnaryOp`

```java
public class CustomizedOp extends UnaryOp {
    private static final long serialVersionUID = 12345678L;

    // Implement this if `NULL` is returned when the parameter is `NULL`.
    @Override
    protected Object evalNonNullValue(@NonNull Object value, ExprConfig config);

    // Implement this if `NULL` parameter need to be processed.
    @Override
    public Object evalValue(Object value, ExprConfig config);

    // Implement this if the parameter (before evaluating, is an `Expr`) need to be processed.
    @Override
    public Object eval(@NonNull Expr expr, EvalContext context, ExprConfig config);

    @Override
    public boolean isConst(@NonNull UnaryOpExpr expr);

    @Override
    public OpKey keyOf(@NonNull Type type);

    @Override
    public OpKey bestKeyOf(@NonNull Type @NonNull [] types);

    @Override
    public UnaryOp getOp(OpKey key);
```

Only one of the methods `evalNonNullValue`, `evalValue` and `eval` is required to be implemented, which do the
evaluating.

The method `isConst` is to predicate if the expression is a const value. If `true`, the `Op` may be replaced by its
evaluated value in compiling time. The default implementation is to see if all the operands of the `Expr` is true. If
the `Op` ought not to be replaced, this method should be overridden and return `false`.

The methods `keyOf`, `bestKeyOf` and `getOp` are used to support multiple signatures of the function and are called in
compiling time. The compiler call these methods as the following steps

1. call `keyOf` to get the `OpKey` for the given types of parameters
2. call `getOp` to get the compiled `Op` for the given `OpKey` (indirectly, for the given types of parameters)
3. if `getOp` returns `null`, which means the given types of parameters is not supported, then try to call `bestKeyOf`
   to decide if type conversions are available

In the implementation of `bestKeyOf`, the required types of parameters should be set into the array, then the
corresponding `OpKey` is returned. The compiler will insert casting `Op` to fulfill the type requirement.

To simplify the implementation of customized function further, the annotation `@Operators` can be used. For example

```java

@Operators
abstract class AddOp extends BinaryOp {
    private static final long serialVersionUID = 7159909541314089027L;

    static int add(int value0, int value1) {
        return value0 + value1;
    }

    static long add(long value0, long value1) {
        return value0 + value1;
    }

    static float add(float value0, float value1) {
        return value0 + value1;
    }

    static double add(double value0, double value1) {
        return value0 + value1;
    }

    @Override
    public @NonNull String getName() {
        return "ADD";
    }

    @Override
    public @Nullable OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return OpKeys.DEFAULT.keyOf(type0, type1);
    }

    @Override
    public final OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        return OpKeys.DEFAULT.bestKeyOf(types);
    }
}
```

where only the evaluating method of different types are required. A final class which extends this class will be
generated, which can wrap these methods into `Op`s.

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

    <!-- Required if annotations are used -->
    <dependency>
        <groupId>io.dingodb.expr</groupId>
        <artifactId>dingo-expr-annotations</artifactId>
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
    // Required if annotations are used
    implementation group: 'io.dingodb.expr', name: 'dingo-expr-annotations', version: '0.7.0'
}
```

## Used By

- [DingoDB](https://github.com/dingodb/dingo)

