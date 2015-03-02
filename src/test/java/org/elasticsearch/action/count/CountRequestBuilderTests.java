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

package org.elasticsearch.action.count;

import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

public class CountRequestBuilderTests extends ElasticsearchTestCase {

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
        CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client);
        assertThat(countRequestBuilder.toString(), equalTo(new QuerySourceBuilder().toString()));
    }

    @Test
    public void testSourceBuilderToString() {
        CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client);
        countRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        assertThat(countRequestBuilder.toString(), equalTo(new QuerySourceBuilder().setQuery(QueryBuilders.matchAllQuery()).toString()));
    }

    @Test
    public void testRequestSourceToString() {
        CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client);
        String source = "{ \"query\" : { \"match_all\" : {} } }";
        countRequestBuilder.setSource(new BytesArray(source));
        assertThat(countRequestBuilder.toString(), equalTo(source));
    }

    @Test
    public void testRequestQueryToString() {
        CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client);
        String query = "{ \"match_all\" : {} }";
        countRequestBuilder.setQuery(new BytesArray(query));
        assertThat(countRequestBuilder.toString(), equalTo("{\n  \"query\":{ \"match_all\" : {} }\n}"));
    }

    @Test
    public void testThatToStringDoesntWipeSource() {
        String source = "{\n" +
                "            \"query\" : {\n" +
                "            \"match\" : {\n" +
                "                \"field\" : {\n" +
                "                    \"query\" : \"value\"" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "        }";
        CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client).setSource(new BytesArray(source));
        String preToString = countRequestBuilder.request().source().toUtf8();
        assertThat(countRequestBuilder.toString(), equalTo(source));
        String postToString = countRequestBuilder.request().source().toUtf8();
        assertThat(preToString, equalTo(postToString));
    }
}
