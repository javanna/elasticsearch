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
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.mockito.Mockito;

public class RequestCallbackTests extends RestClientTestCase {

    public void test() {
        RequestConfig defaultRequestConfig = RequestConfig.DEFAULT;
        int maxRetryTimeoutMillis = 1000;

        HttpHost host1 = new HttpHost("host1", 9200);
        HttpHost host2 = new HttpHost("host2", 9200);
        HttpHost host3 = new HttpHost("host3", 9200);
        HttpHost[] hosts = { host1, host2, host3 };

        RestClient restClient = new RestClient(Mockito.mock(CloseableHttpAsyncClient.class), defaultRequestConfig, maxRetryTimeoutMillis,
                new Header[]{}, hosts, "", new RestClient.FailureListener());

        HttpGet request = new HttpGet("/");

        ResponseListener responseListener = new ResponseListener() {
            @Override
            public void onSuccess(Response response) {

            }

            @Override
            public void onFailure(Exception exception) {

            }
        };

        RestClient.FailureTrackingResponseListener failureTrackingResponseListener =
                new RestClient.FailureTrackingResponseListener(responseListener);

        //new HostTuple

//        restClient.new RequestCallBack(request, host1, Collections.<Integer>emptySet(), failureTrackingResponseListener, )


    }
}
