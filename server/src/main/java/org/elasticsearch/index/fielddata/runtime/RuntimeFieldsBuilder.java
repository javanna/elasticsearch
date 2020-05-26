/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.runtime;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchExtBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RuntimeFieldsBuilder extends SearchExtBuilder {

    public static RuntimeFieldsBuilder fromXContent(XContentParser parser) throws IOException {
        Map<String, String> fields = new HashMap<>();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new ParsingException(parser.getTokenLocation(), "Expected start object " + token);
                }
                while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token != XContentParser.Token.FIELD_NAME) {
                        throw new ParsingException(parser.getTokenLocation(), "Expected field name " + token);
                    }
                    String field = parser.currentName();
                    token = parser.nextToken();
                    if (token != XContentParser.Token.START_OBJECT) {
                        throw new ParsingException(parser.getTokenLocation(), "Expected start object " + token);
                    }
                    while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals("type")) {
                            token = parser.nextToken();
                            fields.put(field, parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "unexpected token " + token);
                        }
                    }
                }
            }
        } else {
            throw new ParsingException(parser.getTokenLocation(), "Expected array " + token);
        }
        if (fields.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "no fields specified for runtime section");
        }
        return new RuntimeFieldsBuilder(fields);
    }

    private final Map<String, String> fields;

    public RuntimeFieldsBuilder(Map<String, String> fields) {
        this.fields = Objects.requireNonNull(fields);
    }

    public RuntimeFieldsBuilder(StreamInput in) throws IOException {
        fields = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(fields, StreamOutput::writeString, StreamOutput::writeString);
    }

    public Map<String, String> fields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RuntimeFieldsBuilder that = (RuntimeFieldsBuilder) o;
        return fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    @Override
    public String getWriteableName() {
        return "runtime_fields";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("runtime_fields");
        for (Map.Entry<String, String> field : fields.entrySet()) {
            builder.startObject(field.getKey());
            builder.field("type", field.getValue());
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }
}
