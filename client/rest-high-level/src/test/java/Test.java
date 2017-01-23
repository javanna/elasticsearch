/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Test {

    public static void main(String[] args) throws IOException {

        String output;
        {
            InternalSearchHit searchHit = new InternalSearchHit(1, "id", new Text("type"), Collections.emptyMap());
            searchHit.sourceRef(new BytesArray("{\"field\":\"value\"}"));
            searchHit.score(10f);
            Map<String, Set<CharSequence>> contexts = new HashMap<>();
            contexts.put("key", Collections.singleton("value"));
            CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(1, new Text("text"), 10, contexts);
            option.setHit(searchHit);
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                option.toXContent(builder, ToXContent.EMPTY_PARAMS);
                output = builder.string();
                System.out.println(output);
            }
        }

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, output)) {
            XContentParser.Token token = parser.nextToken();
            String currentFieldName = null;
            String text = null;
            float score = -1;
            Map<String, Set<CharSequence>> contexts = new HashMap<>();
            InternalSearchHit searchHit;
            try (XContentBuilder builder = JsonXContent.contentBuilder().startObject()) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        if ("text".equals(currentFieldName) == false && "score".equals(currentFieldName) == false
                                && "contexts".equals(currentFieldName) == false) {
                            builder.copyCurrentStructure(parser);
                        }
                    } else if (token.isValue()) {
                        if ("text".equals(currentFieldName)) {
                            text = parser.text();
                        } else if ("score".equals(currentFieldName)) {
                            score = parser.floatValue();
                        } else {
                            throw new IllegalStateException();
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("contexts".equals(currentFieldName)) {
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                                    Set<CharSequence> set = new HashSet<>();
                                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                        set.add(parser.text());
                                    }
                                    contexts.put(currentFieldName, set);
                                } else {
                                    throw new IllegalStateException();
                                }
                            }
                        } else {
                            throw new IllegalStateException();
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        throw new IllegalStateException();
                    }
                }

                BytesReference bytes = builder.endObject().bytes();
                try (XContentParser hitsParser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, bytes)) {
                    hitsParser.nextToken();
                    searchHit = InternalSearchHit.fromXContent(hitsParser);
                }
            }
            CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(1, new Text(text), score, contexts);
            option.setHit(searchHit);
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                option.toXContent(builder, ToXContent.EMPTY_PARAMS);
                System.out.println(builder.string());
            }
        }
    }
}
