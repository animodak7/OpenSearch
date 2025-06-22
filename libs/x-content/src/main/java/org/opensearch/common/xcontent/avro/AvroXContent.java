/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.common.xcontent.avro;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamWriteConstraints;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroGenerator;
import com.fasterxml.jackson.dataformat.avro.AvroParser;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

import org.apache.avro.Schema;
import org.opensearch.common.xcontent.XContentContraints;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentGenerator;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

/**
 * An Avro based content implementation using Jackson.
 */
public class AvroXContent implements XContent, XContentContraints {

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(avroXContent);
    }

    static final AvroFactory avroFactory;
    public static final AvroXContent avroXContent;
    private static final AvroSchema DEFAULT_SCHEMA;
    private static final String DEFAULT_AVRO_SCHEMA = "{"
        + "\"type\": \"record\","
        + "\"name\": \"BulkResponse\","
        + "\"namespace\": \"org.opensearch.common.xcontent.avro\","
        + "\"fields\": ["
        + "    {\"name\": \"took\", \"type\": \"long\"},"
        + "    {\"name\": \"errors\", \"type\": \"boolean\"},"
        + "    {\"name\": \"items\", \"type\": {"
        + "        \"type\": \"array\","
        + "        \"items\": {"
        + "            \"type\": \"record\","
        + "            \"name\": \"BulkResponseItem\","
        + "            \"fields\": ["
        + "                {\"name\": \"index\", \"type\": {"
        + "                    \"type\": \"record\","
        + "                    \"name\": \"IndexResponse\","
        + "                    \"fields\": ["
        + "                        {\"name\": \"_index\", \"type\": \"string\"},"
        + "                        {\"name\": \"_id\", \"type\": \"string\"},"
        + "                        {\"name\": \"_version\", \"type\": [\"null\", \"long\"]},"
        + "                        {\"name\": \"result\", \"type\": [\"null\", \"string\"]},"
        + "                        {\"name\": \"status\", \"type\": \"int\"},"
        + "                        {\"name\": \"error\", \"type\": [\"null\", {"
        + "                            \"type\": \"record\","
        + "                            \"name\": \"ErrorDetails\","
        + "                            \"fields\": ["
        + "                                {\"name\": \"type\", \"type\": \"string\"},"
        + "                                {\"name\": \"reason\", \"type\": \"string\"},"
        + "                                {\"name\": \"caused_by\", \"type\": [\"null\", {"
        + "                                    \"type\": \"record\","
        + "                                    \"name\": \"CausedBy\","
        + "                                    \"fields\": ["
        + "                                        {\"name\": \"type\", \"type\": \"string\"},"
        + "                                        {\"name\": \"reason\", \"type\": \"string\"}"
        + "                                    ]"
        + "                                }]}"
        + "                            ]"
        + "                        }]}"
        + "                    ]"
        + "                }}"
        + "            ]"
        + "        }"
        + "    }}"
        + "]"
        + "}";
//    "{"
//        + "\"type\": \"record\","
//        + "\"name\": \"LogRecord\","
//        + "\"namespace\": \"org.opensearch.common.xcontent.avro\","
//        + "\"fields\": ["
//        + "{\"name\": \"backend_ip\", \"type\": [\"null\", \"string\"]},"
//        + "{\"name\": \"backend_port\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"backend_processing_time\", \"type\": [\"null\", \"float\"]},"
//        + "{\"name\": \"backend_status_code\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"client_ip\", \"type\": [\"null\", \"string\"]},"
//        + "{\"name\": \"client_port\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"connection_time\", \"type\": [\"null\", \"float\"]},"
//        + "{\"name\": \"destination_ip\", \"type\": [\"null\", \"string\"]},"
//        + "{\"name\": \"destination_port\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"elb_status_code\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"http_port\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"http_version\", \"type\": [\"null\", \"string\"]},"
//        + "{\"name\": \"matched_rule_priority\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"received_bytes\", \"type\": [\"null\", \"long\"]},"
//        + "{\"name\": \"request_creation_time\", \"type\": [\"null\", \"long\"]},"
//        + "{\"name\": \"request_processing_time\", \"type\": [\"null\", \"float\"]},"
//        + "{\"name\": \"response_processing_time\", \"type\": [\"null\", \"float\"]},"
//        + "{\"name\": \"sent_bytes\", \"type\": [\"null\", \"long\"]},"
//        + "{\"name\": \"target_ip\", \"type\": [\"null\", \"string\"]},"
//        + "{\"name\": \"target_port\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"target_processing_time\", \"type\": [\"null\", \"float\"]},"
//        + "{\"name\": \"target_status_code\", \"type\": [\"null\", \"int\"]},"
//        + "{\"name\": \"timestamp\", \"type\": [\"null\", \"long\"]}," +
//        "{ \"name\": \"error\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
//        "    { \"name\": \"rootCause\", \"type\": [\"null\", {\n" +
//        "      \"type\": \"array\",\n" +
//        "      \"items\": {\n" +
//        "        \"type\": \"record\",\n" +
//        "        \"name\": \"Cause\",\n" +
//        "        \"fields\": [\n" +
//        "          { \"name\": \"type\", \"type\": [\"null\", \"string\"], \"default\": null },\n" +
//        "          { \"name\": \"reason\", \"type\": [\"null\", \"string\"], \"default\": null }\n" +
//        "        ]\n" +
//        "      }\n" +
//        "    }], \"default\": null }"
//        + "]"
//        + "}";

    static {
        // Initialize default Avro schema

        DEFAULT_SCHEMA = new AvroSchema(new Schema.Parser().parse(DEFAULT_AVRO_SCHEMA));

        avroFactory = AvroFactory.builder()
//            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
//            .disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT)
            .streamWriteConstraints(
                StreamWriteConstraints.builder()
                    .maxNestingDepth(DEFAULT_MAX_DEPTH)
                    .build())
            .streamReadConstraints(
                StreamReadConstraints.builder()
                    .maxStringLength(DEFAULT_MAX_STRING_LEN)
                    .maxNameLength(DEFAULT_MAX_NAME_LEN)
                    .maxNestingDepth(DEFAULT_MAX_DEPTH)
                    .build())
            .build();

        avroXContent = new AvroXContent();
    }

    private AvroXContent() {}

    @Override
    public MediaType mediaType() {
        return XContentType.AVRO; // Define custom type for AVRO
    }

    @Override
    public byte streamSeparator() {
        return (byte) 0xFF;
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        AvroGenerator generator = avroFactory.createGenerator(os);
        if (generator != null) {
            generator.setSchema(DEFAULT_SCHEMA);
        }
        return new AvroXContentGenerator(generator, os, includes, excludes);
    }

    /**
     * Creates a generator with a specific schema
     */
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes, AvroSchema schema)
        throws IOException {
        AvroGenerator generator = avroFactory.createGenerator(os, JsonEncoding.UTF8);
        if (generator != null) {
            generator.setSchema(schema);
        }
        return new AvroXContentGenerator(generator, os, includes, excludes);
    }

    @Override
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        String content) throws IOException {
        JsonParser parser = avroFactory.createParser(content);
        if (parser instanceof com.fasterxml.jackson.dataformat.avro.AvroParser) {
            ((com.fasterxml.jackson.dataformat.avro.AvroParser) parser).setSchema(DEFAULT_SCHEMA);
        }
        return new AvroXContentParser(xContentRegistry, deprecationHandler, parser);
    }

    @Override
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        InputStream is) throws IOException {
        AvroParser parser = avroFactory.createParser(is);
        if (parser != null) {
            parser.setSchema(DEFAULT_SCHEMA);
        }
        return new AvroXContentParser(xContentRegistry, deprecationHandler, parser);
    }

    @Override
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        byte[] data) throws IOException {
        AvroParser parser = avroFactory.createParser(data);
        if (parser != null) {
            parser.setSchema(DEFAULT_SCHEMA);
        }
        return new AvroXContentParser(xContentRegistry, deprecationHandler, parser);
    }

    @Override
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        byte[] data,
        int offset,
        int length) throws IOException {
        AvroParser parser = avroFactory.createParser(data, offset, length);
        if (parser != null) {
            parser.setSchema(DEFAULT_SCHEMA);
        }
        return new AvroXContentParser(xContentRegistry, deprecationHandler, parser);
    }

    @Override
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        Reader reader) throws IOException {
        JsonParser parser = avroFactory.createParser(reader);
        if (parser instanceof com.fasterxml.jackson.dataformat.avro.AvroParser) {
            ((com.fasterxml.jackson.dataformat.avro.AvroParser) parser).setSchema(DEFAULT_SCHEMA);
        }
        return new AvroXContentParser(xContentRegistry, deprecationHandler, parser);
    }

    /**
     * Creates a parser with a specific schema
     */
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        InputStream is,
        AvroSchema schema) throws IOException {
        AvroParser parser = avroFactory.createParser(is);
        if (parser != null) {
            parser.setSchema(schema);
        }
        return new AvroXContentParser(xContentRegistry, deprecationHandler, parser);
    }
}
