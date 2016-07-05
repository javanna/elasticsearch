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
import org.apache.http.HttpResponse;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Integration test to check interaction between {@link RestAsyncClient} and {@link org.apache.http.nio.client.HttpAsyncClient}.
 * Works against a real http server, one single host.
 */
//animal-sniffer doesn't like our usage of com.sun.net.httpserver.* classes
@IgnoreJRERequirement
public class RestAsyncClientIntegTests extends RestClientIntegTestCase {

    private static RestAsyncClient restClient;

    @BeforeClass
    public static void setupRestClient() throws Exception {
        RestAsyncClient.Builder builder = RestAsyncClient.builder(
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
        HttpAsyncResponseConsumer<HttpResponse> responseConsumer = HttpAsyncMethods.createConsumer();
        SyncResponseListener syncResponseListener = new SyncResponseListener();
        restClient.performRequest(method, endpoint, params, entity, responseConsumer, syncResponseListener, headers);
        return syncResponseListener.get();
    }

    @Override
    protected Set<String> getStandardHeaders(String method) {
        Set<String> standardHeaders = new HashSet<>(Arrays.asList("Connection", "Host", "User-agent", "Date"));
        if (method.equals("HEAD") == false) {
            standardHeaders.add("Content-length");
        }
        return standardHeaders;
    }

    private static class SyncResponseListener implements RestAsyncClient.ResponseListener {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile Response response;
        private volatile Exception exception;

        @Override
        public void onSuccess(Response response) {
            this.response = response;
            latch.countDown();
        }

        @Override
        public void onFailure(Exception exception) {
            this.exception = exception;
            latch.countDown();
        }

        Response get() throws Exception {
            latch.await();
            if (this.response != null) {
                return this.response;
            }
            throw this.exception;
        }
    }
}
