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

package org.elasticsearch.action.search;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

public class SearchRequestBuilderTests extends ElasticsearchTestCase {

    private static Client client;

    @BeforeClass
    public static void initClient() {
        //this client will not be hit by any request, but it needs to be a non null proper client
        //that is why we create it but we don't add any transport address to it
        client = new TransportClient();
    }

    @AfterClass
    public static void closeClient() {
        client.close();
        client = null;
    }

    @Test
    public void testEmptyToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().toString()));
    }

    @Test
    public void testSourceBuilderAndExtraSourceMergeToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery()).setSize(25).setFrom(100);
        searchRequestBuilder.setExtraSource("{\"size\": 50}");
        String finalSource = "{\n  \"query\" : {\n    \"match_all\" : { }\n  },\n  \"from\" : 100,\n  \"size\" : 50\n}";
        assertThat(searchRequestBuilder.toString(), equalTo(finalSource));
    }

    @Test
    public void testSourceBuilderAndExtraSourceParseErrorToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery()).setSize(25).setFrom(100);
        searchRequestBuilder.setExtraSource("{\"size\" 50");
        assertThat(searchRequestBuilder.toString(), equalTo("{ \"error\" : \"Failed to parse content to map\"}"));
    }

    @Test
    public void testSourceBuilderToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()));
    }

    @Test
    public void testRequestQueryToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        String query = "{ \"match_all\" : {} }";
        searchRequestBuilder.setQuery(query);
        assertThat(searchRequestBuilder.toString(), equalTo("{\n  \"query\":{ \"match_all\" : {} }\n}"));
    }

    @Test
    public void testRequestSourceToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        String source = "{ \"query\" : { \"match_all\" : {} } }";
        searchRequestBuilder.setSource(source);
        assertThat(searchRequestBuilder.toString(), equalTo(source));
    }

    @Test
    public void testRequestExtraSourceToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        String extraSource = "{ \"from\": 10, \"size\":20}";
        searchRequestBuilder.setExtraSource(extraSource);
        assertThat(searchRequestBuilder.toString(), equalTo(extraSource));
    }

    @Test
    public void testRequestSourceAndExtraSourceMergeToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        String source = "{ \"size\":50, \"query\" : { \"match_all\" : {} } }";
        searchRequestBuilder.setSource(source);
        String extraSource = "{ \"from\": 10, \"size\":20}";
        searchRequestBuilder.setExtraSource(extraSource);
        String finalSource = "{\n  \"query\" : {\n    \"match_all\" : { }\n  },\n  \"from\" : 10,\n  \"size\" : 20\n}";
        assertThat(searchRequestBuilder.toString(), equalTo(finalSource));
    }

    @Test
    public void testRequestSourceAndExtraSourceParseErrorToString() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        String source = "{\"size\":50 \"query\" : { \"match_all\" : {} } }";
        searchRequestBuilder.setSource(source);
        String extraSource = "{ \"from\": 10, \"size\":20}";
        searchRequestBuilder.setExtraSource(extraSource);
        assertThat(searchRequestBuilder.toString(), equalTo("{ \"error\" : \"Failed to parse content to map\"}"));
    }

    @Test
    public void testThatToStringDoesntWipeRequestSource() {
        String source = "{\n" +
                "            \"query\" : {\n" +
                "            \"match\" : {\n" +
                "                \"field\" : {\n" +
                "                    \"query\" : \"value\"" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "        }";
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client).setSource(source);
        String preToString = searchRequestBuilder.request().source().toUtf8();
        assertThat(searchRequestBuilder.toString(), equalTo(source));
        String postToString = searchRequestBuilder.request().source().toUtf8();
        assertThat(preToString, equalTo(postToString));
    }
}
