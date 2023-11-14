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

package io.dingodb.expr.json.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.dingodb.expr.runtime.type.AnyType;
import io.dingodb.expr.runtime.type.ArrayType;
import io.dingodb.expr.runtime.type.BoolType;
import io.dingodb.expr.runtime.type.BytesType;
import io.dingodb.expr.runtime.type.DecimalType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.expr.runtime.type.ListType;
import io.dingodb.expr.runtime.type.LongType;
import io.dingodb.expr.runtime.type.MapType;
import io.dingodb.expr.runtime.type.NullType;
import io.dingodb.expr.runtime.type.StringType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.TypeVisitorBase;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class DataParser extends Parser {
    private static final long serialVersionUID = -6849693677072717377L;

    private final SchemaRoot schemaRoot;

    private DataParser(DataFormat format, SchemaRoot schemaRoot) {
        super(format);
        this.schemaRoot = schemaRoot;
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Create a {@link DataParser} of json format.
     *
     * @param schemaRoot the {@link SchemaRoot}
     * @return a {@link DataParser}
     */
    public static @NonNull DataParser json(SchemaRoot schemaRoot) {
        return new DataParser(DataFormat.APPLICATION_JSON, schemaRoot);
    }

    /**
     * Create a {@link DataParser} of yaml format.
     *
     * @param schemaRoot the {@link SchemaRoot}
     * @return a {@link DataParser}
     */
    public static @NonNull DataParser yaml(SchemaRoot schemaRoot) {
        return new DataParser(DataFormat.APPLICATION_YAML, schemaRoot);
    }

    /**
     * Create a {@link DataParser} of a specified format.
     *
     * @param format     the {@link DataFormat}
     * @param schemaRoot the {@link SchemaRoot}
     * @return a {@link DataParser}
     */
    public static @NonNull DataParser get(DataFormat format, SchemaRoot schemaRoot) {
        return new DataParser(format, schemaRoot);
    }

    private static @Nullable Object jsonNodeValue(@NonNull JsonNode jsonNode) {
        JsonNodeType type = jsonNode.getNodeType();
        switch (type) {
            case NUMBER:
                if (jsonNode.isInt() || jsonNode.isLong()) {
                    return jsonNode.asLong();
                }
                return jsonNode.asDouble();
            case STRING:
                return jsonNode.asText();
            case BOOLEAN:
                return jsonNode.asBoolean();
            case ARRAY:
                List<Object> list = new LinkedList<>();
                for (int i = 0; i < jsonNode.size(); i++) {
                    list.add(jsonNodeValue(jsonNode.get(i)));
                }
                return list;
            case OBJECT:
                Map<String, Object> map = new HashMap<>(jsonNode.size());
                Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    map.put(entry.getKey(), jsonNodeValue(entry.getValue()));
                }
                return map;
            case NULL:
                return null;
            default:
                break;
        }
        throw new IllegalArgumentException("Unsupported json node type \"" + type + "\".");
    }

    /**
     * Parse a given string into a tuple.
     *
     * @param text the given string
     * @return the tuple
     * @throws JsonProcessingException if something is wrong
     */
    public Object @NonNull [] parse(String text) throws JsonProcessingException {
        JsonNode jsonNode = mapper.readTree(text);
        return jsonNodeToTuple(jsonNode);
    }

    /**
     * Read from a given {@link InputStream} and parse the contents into a tuple.
     *
     * @param is the given {@link InputStream}
     * @return the tuple
     * @throws IOException if something is wrong
     */
    public Object @NonNull [] parse(InputStream is) throws IOException {
        JsonNode jsonNode = mapper.readTree(new InputStreamReader(is));
        return jsonNodeToTuple(jsonNode);
    }

    /**
     * Serialize a tuple into a string.
     *
     * @param tuple the tuple
     * @return the serialized string
     * @throws JsonProcessingException if something is wrong
     */
    public String serialize(Object[] tuple) throws JsonProcessingException {
        Object object = TupleSerializer.INSTANCE.visit(schemaRoot.getSchema(), tuple);
        return mapper.writeValueAsString(object);
    }

    private Object @NonNull [] jsonNodeToTuple(@NonNull JsonNode jsonNode) {
        @NonNull DataSchema dataSchema = schemaRoot.getSchema();
        TupleReader visitor = new TupleReader(schemaRoot.getMaxIndex());
        visitor.visit(dataSchema, jsonNode);
        return visitor.getTuple();
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class TupleSerializer implements DataSchemaVisitor<Object, Object @NonNull []> {
        private static final TupleSerializer INSTANCE = new TupleSerializer();

        @Override
        public Object visitLeaf(@NonNull DataLeaf schema, Object @NonNull [] obj) {
            return obj[schema.getIndex()];
        }

        @Override
        public @NonNull Object visitTuple(@NonNull DataTuple schema, Object @NonNull [] obj) {
            List<Object> list = new LinkedList<>();
            for (int i = 0; i < schema.getChildren().length; i++) {
                list.add(visit(schema.getChild(i), obj));
            }
            return list;
        }

        @Override
        public @NonNull Object visitDict(@NonNull DataDict schema, Object @NonNull [] obj) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (Map.Entry<String, DataSchema> entry : schema.getChildren().entrySet()) {
                map.put(entry.getKey(), visit(entry.getValue(), obj));
            }
            return map;
        }
    }

    private static class TupleReader implements DataSchemaVisitor<@Nullable Void, JsonNode> {
        @Getter
        private final Object[] tuple;

        public TupleReader(int size) {
            this.tuple = new Object[size];
        }

        @Override
        public @Nullable Void visitLeaf(@NonNull DataLeaf schema, JsonNode obj) {
            tuple[schema.getIndex()] = JsonValueReader.INSTANCE.visit(schema.getType(), obj);
            return null;
        }

        @Override
        public @Nullable Void visitTuple(@NonNull DataTuple schema, JsonNode obj) {
            for (int i = 0; i < schema.getChildren().length; i++) {
                JsonNode item = obj.get(i);
                if (item != null) {
                    visit(schema.getChild(i), item);
                }
            }
            return null;
        }

        @Override
        public @Nullable Void visitDict(@NonNull DataDict schema, JsonNode obj) {
            for (Map.Entry<String, DataSchema> entry : schema.getChildren().entrySet()) {
                String key = entry.getKey();
                JsonNode child = obj.get(key);
                if (child != null) {
                    visit(entry.getValue(), child);
                }
            }
            return null;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class JsonValueReader extends TypeVisitorBase<Object, @NonNull JsonNode> {
        private static final JsonValueReader INSTANCE = new JsonValueReader();

        @Override
        public Object visit(@NonNull Type type, @NonNull JsonNode obj) {
            return !obj.isNull() ? type.accept(this, obj) : null;
        }

        @Override
        public Object visitNullType(@NonNull NullType type, @NonNull JsonNode obj) {
            return null;
        }

        @Override
        public Object visitIntType(@NonNull IntType type, @NonNull JsonNode obj) {
            return obj.asInt();
        }

        @Override
        public Object visitLongType(@NonNull LongType type, @NonNull JsonNode obj) {
            return obj.asLong();
        }

        @Override
        public Object visitFloatType(@NonNull FloatType type, @NonNull JsonNode obj) {
            return obj.floatValue();
        }

        @Override
        public Object visitDoubleType(@NonNull DoubleType type, @NonNull JsonNode obj) {
            return obj.asDouble();
        }

        @Override
        public Object visitBoolType(@NonNull BoolType type, @NonNull JsonNode obj) {
            return obj.asBoolean();
        }

        @Override
        public Object visitDecimalType(@NonNull DecimalType type, @NonNull JsonNode obj) {
            return obj.decimalValue();
        }

        @Override
        public Object visitStringType(@NonNull StringType type, @NonNull JsonNode obj) {
            return obj.asText();
        }

        @SneakyThrows(IOException.class)
        @Override
        public Object visitBytesType(@NonNull BytesType type, @NonNull JsonNode obj) {
            return obj.binaryValue();
        }

        @Override
        public Object visitAnyType(@NonNull AnyType type, @NonNull JsonNode obj) {
            return jsonNodeValue(obj);
        }

        @Override
        public Object visitArrayType(@NonNull ArrayType type, @NonNull JsonNode obj) {
            Type elementType = type.getElementType();
            Object array = ArrayBuilder.INSTANCE.visit(elementType, obj.size());
            for (int i = 0; i < obj.size(); i++) {
                Array.set(array, i, visit(elementType, obj.get(i)));
            }
            return array;
        }

        @Override
        public Object visitListType(@NonNull ListType type, @NonNull JsonNode obj) {
            return jsonNodeValue(obj);
        }

        @Override
        public Object visitMapType(@NonNull MapType type, @NonNull JsonNode obj) {
            return jsonNodeValue(obj);
        }
    }
}
