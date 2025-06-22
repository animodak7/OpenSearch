/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.common.xcontent.avro;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.*;

/**
 * Avro based content parser using Jackson.
 */
public class AvroXContentParser extends JsonXContentParser {
    final JsonParser parser;
    private final NamedXContentRegistry xContentRegistry;
    private final DeprecationHandler deprecationHandler;

    public AvroXContentParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, JsonParser parser) {
        super(xContentRegistry, deprecationHandler, parser);
        this.xContentRegistry = xContentRegistry;
        this.deprecationHandler = deprecationHandler;
        this.parser = parser;
    }

    @Override
    public XContentType contentType() {
        return XContentType.AVRO; // Or extend MediaType to include AVRO
    }
//
//    @Override
//    public Token nextToken() throws IOException {
//        return convertToken(parser.nextToken());
//    }
//
//    @Override
//    public void skipChildren() throws IOException {
//        parser.skipChildren();
//    }
//
//    @Override
//    public Token currentToken() {
//        return convertToken(parser.currentToken());
//    }
//
//    @Override
//    public String currentName() throws IOException {
//        return parser.currentName();
//    }
//
//    @Override
//    public Map<String, Object> map() throws IOException {
//        return readMap(parser);
//    }
//
//    @Override
//    public Map<String, Object> mapOrdered() throws IOException {
//        return readOrderedMap(parser);
//    }
//
//    @Override
//    public Map<String, String> mapStrings() throws IOException {
//        return readMapStrings(parser);
//    }
//
//    public Map<String, String> mapStringsOrdered() throws IOException {
//        return readOrderedMapStrings(parser);
//    }
//
//    @Override
//    public String text() throws IOException {
//        if (parser.currentToken() == JsonToken.VALUE_NULL) {
//            return null;
//        }
//        return parser.getText();
//    }
//
//    @Override
//    public String textOrNull() throws IOException {
//        return parser.getText();
//    }
//
//    @Override
//    public CharBuffer charBuffer() throws IOException {
//        return new CharBuffer(text());
//    }
//
//    @Override
//    public Object objectText() throws IOException {
//        JsonToken currentToken = parser.currentToken();
//        if (currentToken == JsonToken.VALUE_NULL) {
//            return null;
//        }
//        if (currentToken == JsonToken.VALUE_STRING) {
//            return text();
//        }
//        return textOrNull();
//    }
//
//    @Override
//    public Object objectBytes() throws IOException {
//        JsonToken currentToken = parser.currentToken();
//        if (currentToken == JsonToken.VALUE_NULL) {
//            return null;
//        }
//        if (currentToken == JsonToken.VALUE_STRING) {
//            return text().getBytes("UTF-8");
//        }
//        return textOrNull();
//    }
//
//    @Override
//    public boolean hasTextCharacters() {
//        return parser.hasTextCharacters();
//    }
//
//    @Override
//    public char[] textCharacters() throws IOException {
//        return parser.getTextCharacters();
//    }
//
//    @Override
//    public int textLength() throws IOException {
//        return parser.getTextLength();
//    }
//
//    @Override
//    public int textOffset() throws IOException {
//        return parser.getTextOffset();
//    }
//
//    @Override
//    public Number numberValue() throws IOException {
//        return parser.getNumberValue();
//    }
//
//    @Override
//    public NumberType numberType() throws IOException {
//        return convertNumberType(parser.getNumberType());
//    }
//
//    @Override
//    public byte[] binaryValue() throws IOException {
//        return parser.getBinaryValue();
//    }
//
//    @Override
//    public short shortValue(boolean coerce) throws IOException {
//        return numberValue().shortValue();
//    }
//
//    @Override
//    public int intValue(boolean coerce) throws IOException {
//        return numberValue().intValue();
//    }
//
//    @Override
//    public long longValue(boolean coerce) throws IOException {
//        return numberValue().longValue();
//    }
//
//    @Override
//    public float floatValue(boolean coerce) throws IOException {
//        return numberValue().floatValue();
//    }
//
//    @Override
//    public double doubleValue(boolean coerce) throws IOException {
//        return numberValue().doubleValue();
//    }
//
//    @Override
//    public boolean booleanValue() throws IOException {
//        JsonToken currentToken = parser.currentToken();
//        if (currentToken == JsonToken.VALUE_TRUE) {
//            return true;
//        }
//        if (currentToken == JsonToken.VALUE_FALSE) {
//            return false;
//        }
//        throw new IllegalStateException("Current token is not a boolean: " + currentToken);
//    }
//
//    public String[] textArray() throws IOException {
//        Token token = currentToken();
//        if (token == Token.VALUE_NULL) {
//            return null;
//        }
//        if (token != Token.START_ARRAY) {
//            throw new IllegalStateException("Current token not START_ARRAY: " + token);
//        }
//
//        ArrayList<String> list = new ArrayList<>();
//        while ((token = nextToken()) != Token.END_ARRAY) {
//            list.add(text());
//        }
//        return list.toArray(new String[list.size()]);
//    }
//
//    @Override
//    public NamedXContentRegistry getXContentRegistry() {
//        return xContentRegistry;
//    }
//
//    @Override
//    public DeprecationHandler getDeprecationHandler() {
//        return deprecationHandler;
//    }
//
//    @Override
//    public void close() throws IOException {
//        parser.close();
//    }
//
//    private static Token convertToken(JsonToken token) {
//        if (token == null) {
//            return null;
//        }
//        switch (token) {
//            case FIELD_NAME:
//                return Token.FIELD_NAME;
//            case VALUE_STRING:
//                return Token.VALUE_STRING;
//            case VALUE_NUMBER_INT:
//            case VALUE_NUMBER_FLOAT:
//                return Token.VALUE_NUMBER;
//            case VALUE_TRUE:
//            case VALUE_FALSE:
//                return Token.VALUE_BOOLEAN;
//            case VALUE_NULL:
//                return Token.VALUE_NULL;
//            case START_OBJECT:
//                return Token.START_OBJECT;
//            case END_OBJECT:
//                return Token.END_OBJECT;
//            case START_ARRAY:
//                return Token.START_ARRAY;
//            case END_ARRAY:
//                return Token.END_ARRAY;
//            case VALUE_EMBEDDED_OBJECT:
//                return Token.VALUE_EMBEDDED_OBJECT;
//            default:
//                throw new IllegalStateException("Unexpected token: " + token);
//        }
//    }
//
//    private static NumberType convertNumberType(JsonParser.NumberType numberType) {
//        switch (numberType) {
//            case INT:
//                return NumberType.INT;
//            case LONG:
//                return NumberType.LONG;
//            case FLOAT:
//                return NumberType.FLOAT;
//            case DOUBLE:
//                return NumberType.DOUBLE;
//            default:
//                throw new IllegalStateException("Unexpected number type: " + numberType);
//        }
//    }
//
//    private static Map<String, Object> readMap(JsonParser parser) throws IOException {
//        Map<String, Object> map = new HashMap<>();
//        JsonToken token;
//        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
//            if (token != JsonToken.FIELD_NAME) {
//                throw new IllegalStateException("Expected field name but got " + token);
//            }
//            String fieldName = parser.getCurrentName();
//            token = parser.nextToken();
//            Object value = readValue(parser, token);
//            map.put(fieldName, value);
//        }
//        return map;
//    }
//
//    private static Map<String, Object> readOrderedMap(JsonParser parser) throws IOException {
//        Map<String, Object> map = new LinkedHashMap<>();
//        JsonToken token;
//        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
//            if (token != JsonToken.FIELD_NAME) {
//                throw new IllegalStateException("Expected field name but got " + token);
//            }
//            String fieldName = parser.getCurrentName();
//            token = parser.nextToken();
//            Object value = readValue(parser, token);
//            map.put(fieldName, value);
//        }
//        return map;
//    }
//
//    private static Map<String, String> readMapStrings(JsonParser parser) throws IOException {
//        Map<String, String> map = new HashMap<>();
//        JsonToken token;
//        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
//            if (token != JsonToken.FIELD_NAME) {
//                throw new IllegalStateException("Expected field name but got " + token);
//            }
//            String fieldName = parser.getCurrentName();
//            token = parser.nextToken();
//            String value = readStringValue(parser, token);
//            map.put(fieldName, value);
//        }
//        return map;
//    }
//
//    private static Map<String, String> readOrderedMapStrings(JsonParser parser) throws IOException {
//        Map<String, String> map = new LinkedHashMap<>();
//        JsonToken token;
//        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
//            if (token != JsonToken.FIELD_NAME) {
//                throw new IllegalStateException("Expected field name but got " + token);
//            }
//            String fieldName = parser.getCurrentName();
//            token = parser.nextToken();
//            String value = readStringValue(parser, token);
//            map.put(fieldName, value);
//        }
//        return map;
//    }
//
//    private static Object readValue(JsonParser parser, JsonToken token) throws IOException {
//        switch (token) {
//            case VALUE_NULL:
//                return null;
//            case VALUE_STRING:
//                return parser.getText();
//            case VALUE_NUMBER_INT:
//                return parser.getLongValue();
//            case VALUE_NUMBER_FLOAT:
//                return parser.getDoubleValue();
//            case VALUE_TRUE:
//                return Boolean.TRUE;
//            case VALUE_FALSE:
//                return Boolean.FALSE;
//            case START_OBJECT:
//                return readMap(parser);
//            case START_ARRAY:
//                return readList(parser);
//            case VALUE_EMBEDDED_OBJECT:
//                return parser.getBinaryValue();
//            default:
//                throw new IllegalStateException("Unexpected token: " + token);
//        }
//    }
//
//    private static String readStringValue(JsonParser parser, JsonToken token) throws IOException {
//        if (token == JsonToken.VALUE_NULL) {
//            return null;
//        }
//        if (token != JsonToken.VALUE_STRING) {
//            throw new IllegalStateException("Expected string value but got " + token);
//        }
//        return parser.getText();
//    }
//
//    private static List<Object> readList(JsonParser parser) throws IOException {
//        List<Object> list = new ArrayList<>();
//        JsonToken token;
//        while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
//            Object value = readValue(parser, token);
//            list.add(value);
//        }
//        return list;
//    }
//
//    // Avro-specific methods
//    public AvroParser getAvroParser() {
//        if (parser instanceof AvroParser) {
//            return (AvroParser) parser;
//        }
//        throw new IllegalStateException("Not an Avro parser");
//    }
}
