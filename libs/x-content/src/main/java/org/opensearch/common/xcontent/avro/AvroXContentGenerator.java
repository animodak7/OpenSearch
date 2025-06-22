/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.common.xcontent.avro;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.filter.FilteringGeneratorDelegate;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.dataformat.avro.AvroGenerator;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContentGenerator;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentGenerator;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.filtering.FilterPathBasedFilter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Set;

/**
 * Avro based content generator using Jackson.
 */
public class AvroXContentGenerator extends JsonXContentGenerator {

    public AvroXContentGenerator(JsonGenerator jsonGenerator, OutputStream os, Set<String> includes, Set<String> excludes) {
        super(jsonGenerator, os, includes, excludes);
//        Objects.requireNonNull(includes, "Including filters must not be null");
//        Objects.requireNonNull(excludes, "Excluding filters must not be null");
//
//        this.os = os;
//        if (jsonGenerator instanceof GeneratorBase) {
//            this.base = (GeneratorBase) jsonGenerator;
//        } else {
//            this.base = null;
//        }
//
//        JsonGenerator generator = jsonGenerator;
//
//        boolean hasExcludes = !excludes.isEmpty();
//        if (hasExcludes) {
//            generator = new FilteringGeneratorDelegate(generator, new FilterPathBasedFilter(excludes, false), true, true);
//        }
//
//        boolean hasIncludes = !includes.isEmpty();
//        if (hasIncludes) {
//            generator = new FilteringGeneratorDelegate(generator, new FilterPathBasedFilter(includes, true), true, true);
//        }
//
//        if (hasExcludes || hasIncludes) {
//            this.filter = (FilteringGeneratorDelegate) generator;
//        } else {
//            this.filter = null;
//        }
//        this.generator = generator;
    }

    @Override
    public XContentType contentType() {
        return XContentType.AVRO; // Or extend MediaType to include AVRO
    }

//    @Override
//    public void usePrettyPrint() {
//        // Pretty print not supported for binary format
//        this.prettyPrint = false;
//    }
//
//    @Override
//    public boolean isPrettyPrint() {
//        return prettyPrint;
//    }
//
//    @Override
//    public void usePrintLineFeedAtEnd() {
//        this.writeLineFeedAtEnd = true;
//    }
//
//    private boolean isFiltered() {
//        return filter != null;
//    }
//
//    private JsonGenerator getLowLevelGenerator() {
//        if (isFiltered()) {
//            return filter.getDelegate();
//        }
//        return generator;
//    }
//
//    @Override
//    public void writeStartObject() throws IOException {
//        generator.writeStartObject();
//    }
//
//    @Override
//    public void writeEndObject() throws IOException {
//        generator.writeEndObject();
//    }
//
//    @Override
//    public void writeStartArray() throws IOException {
//        generator.writeStartArray();
//    }
//
//    @Override
//    public void writeEndArray() throws IOException {
//        generator.writeEndArray();
//    }
//
//    @Override
//    public void writeFieldName(String name) throws IOException {
//        generator.writeFieldName(name);
//    }
//
//    @Override
//    public void writeNull() throws IOException {
//        generator.writeNull();
//    }
//
//    @Override
//    public void writeNullField(String name) throws IOException {
//        generator.writeNullField(name);
//    }
//
//    @Override
//    public void writeBooleanField(String name, boolean value) throws IOException {
//        generator.writeBooleanField(name, value);
//    }
//
//    @Override
//    public void writeBoolean(boolean value) throws IOException {
//        generator.writeBoolean(value);
//    }
//
//    @Override
//    public void writeNumberField(String name, double value) throws IOException {
//        generator.writeNumberField(name, value);
//    }
//
//    @Override
//    public void writeNumber(double value) throws IOException {
//        generator.writeNumber(value);
//    }
//
//    @Override
//    public void writeNumberField(String name, float value) throws IOException {
//        generator.writeNumberField(name, value);
//    }
//
//    @Override
//    public void writeNumber(float value) throws IOException {
//        generator.writeNumber(value);
//    }
//
//    @Override
//    public void writeNumberField(String name, int value) throws IOException {
//        generator.writeNumberField(name, value);
//    }
//
//    @Override
//    public void writeNumber(int value) throws IOException {
//        generator.writeNumber(value);
//    }
//
//    @Override
//    public void writeNumberField(String name, long value) throws IOException {
//        generator.writeNumberField(name, value);
//    }
//
//    @Override
//    public void writeNumber(long value) throws IOException {
//        generator.writeNumber(value);
//    }
//
//    @Override
//    public void writeNumber(short value) throws IOException {
//        generator.writeNumber(value);
//    }
//
//    @Override
//    public void writeNumber(BigInteger value) throws IOException {
//        generator.writeNumber(value);
//    }
//
//    @Override
//    public void writeNumberField(String name, BigInteger value) throws IOException {
//        generator.writeFieldName(name);
//        generator.writeNumber(value);
//    }
//
//    @Override
//    public void writeNumber(BigDecimal value) throws IOException {
//        generator.writeNumber(value);
//    }
//
//    @Override
//    public void writeNumberField(String name, BigDecimal value) throws IOException {
//        generator.writeNumberField(name, value);
//    }
//
//    @Override
//    public void writeStringField(String name, String value) throws IOException {
//        generator.writeStringField(name, value);
//    }
//
//    @Override
//    public void writeString(String value) throws IOException {
//        generator.writeString(value);
//    }
//
//    @Override
//    public void writeString(char[] text, int offset, int len) throws IOException {
//        generator.writeString(text, offset, len);
//    }
//
//    @Override
//    public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
//        generator.writeUTF8String(value, offset, length);
//    }
//
//    @Override
//    public void writeBinaryField(String name, byte[] value) throws IOException {
//        generator.writeBinaryField(name, value);
//    }
//
//    @Override
//    public void writeBinary(byte[] value) throws IOException {
//        generator.writeBinary(value);
//    }
//
//    @Override
//    public void writeBinary(byte[] value, int offset, int length) throws IOException {
//        generator.writeBinary(value, offset, length);
//    }
//
//    @Override
//    @Deprecated
//    public void writeRawField(String name, InputStream content) throws IOException {
//        writeFieldName(name);
//        generator.writeRaw(':');
//        copyRawContent(content, generator);
//    }
//
//    @Override
//    public void writeRawField(String name, InputStream content, MediaType mediaType) throws IOException {
//        writeFieldName(name);
//        generator.writeRaw(':');
//        copyRawContent(content, generator);
//    }
//
//    private void copyRawContent(InputStream content, JsonGenerator generator) throws IOException {
//        byte[] buffer = new byte[1024];
//        int read;
//        while ((read = content.read(buffer)) != -1) {
//            generator.writeRaw(new String(buffer, 0, read));
//        }
//    }
//
//    @Override
//    public void writeRawValue(InputStream value, MediaType mediaType) throws IOException {
//        copyRawContent(value, generator);
//    }
//
//    @Override
//    public void copyCurrentStructure(XContentParser parser) throws IOException {
//        if (parser instanceof AvroXContentParser) {
//            generator.copyCurrentStructure(((AvroXContentParser) parser).parser);
//        } else {
//            // Generic structure copying for non-Avro parsers
//            XContentParser.Token token = parser.currentToken();
//            if (token == XContentParser.Token.FIELD_NAME) {
//                writeFieldName(parser.currentName());
//                token = parser.nextToken();
//            }
//
//            switch (token) {
//                case START_ARRAY:
//                    writeStartArray();
//                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
//                        copyCurrentStructure(parser);
//                    }
//                    writeEndArray();
//                    break;
//                case START_OBJECT:
//                    writeStartObject();
//                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
//                        copyCurrentStructure(parser);
//                    }
//                    writeEndObject();
//                    break;
//                default:
//                    copyCurrentEvent(parser);
//            }
//        }
//    }
//
//    @Override
//    public void flush() throws IOException {
//        generator.flush();
//    }
//
//    @Override
//    public void close() throws IOException {
//        if (generator.isClosed()) {
//            return;
//        }
//        JsonStreamContext context = generator.getOutputContext();
//        if (context != null && !context.inRoot()) {
//            throw new IOException("Unclosed object or array found");
//        }
//        if (writeLineFeedAtEnd) {
//            flush();
//            getLowLevelGenerator().writeRaw(LF);
//        }
//        generator.close();
//    }
//
//    @Override
//    public boolean isClosed() {
//        return generator.isClosed();
//    }
}
