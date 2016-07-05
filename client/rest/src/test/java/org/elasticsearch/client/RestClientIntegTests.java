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

package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Integration test to check interaction between {@link RestClient} and {@link org.apache.http.client.HttpClient}.
 * Works against a real http server, one single host.
 */
//animal-sniffer doesn't like our usage of com.sun.net.httpserver.* classes
@IgnoreJRERequirement
public class RestClientIntegTests extends RestClientIntegTestCase {

    private static RestClient restClient;

    @BeforeClass
    public static void setupRestClient() {
        RestClient.Builder builder = RestClient.builder(
                new HttpHost(httpServer.getAddress().getHostString(), httpServer.getAddress().getPort()));
        builder.setDefaultHeaders(defaultHeaders);
        restClient = builder.build();
    }

    @AfterClass
    public static void closeRestClient() throws IOException {
        restClient.close();
        restClient = null;
    }

    @Override
    protected Response performRequest(String method, String endpoint, Map<String, String> params,
                                      HttpEntity entity, Header... headers) throws Exception {
        return restClient.performRequest(method, endpoint, params, entity, headers);
    }

    @Override
    protected Set<String> getStandardHeaders(String method) {
        Set<String> standardHeaders = new HashSet<>(
                Arrays.asList("Accept-encoding", "Connection", "Host", "User-agent", "Date"));
        if (method.equals("HEAD") == false) {
            standardHeaders.add("Content-length");
        }
        return standardHeaders;
    }
}
