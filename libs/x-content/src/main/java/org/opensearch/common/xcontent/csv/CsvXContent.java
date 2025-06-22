/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent.csv;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import org.opensearch.common.xcontent.*;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import org.opensearch.core.xcontent.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

public class CsvXContent implements XContent {

    static final CsvFactory csvFactory;
    static final JsonFactory jsonFactory;
    public static final CsvXContent csvXContent;


    static {
        csvFactory = new CsvFactory();
        jsonFactory = new JsonFactory();

        csvXContent = new CsvXContent();
    }


    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(csvXContent);
    }

//    @Override
//    public XContentType type() {
//        return XContentType.CSV;
//    }

    @Override
    public MediaType mediaType() {
        return null;
    }

    @Override
    public byte streamSeparator() {
        return '\n';
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new CsvXContentGenerator(jsonFactory.createGenerator(os, JsonEncoding.UTF8), os, includes, excludes);
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, String content) throws IOException {
        return null;
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, InputStream is) throws IOException {
        return null;
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, byte[] data) throws IOException {
        return null;
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, byte[] data, int offset, int length) throws IOException {
        return null;
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, Reader reader) throws IOException {
        return null;
    }

    private CsvXContent() {}
}
