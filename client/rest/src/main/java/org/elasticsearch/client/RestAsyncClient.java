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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class RestAsyncClient extends AbstractRestClient {

    private static final Log logger = LogFactory.getLog(RestAsyncClient.class);

    private final CloseableHttpAsyncClient client;

    private RestAsyncClient(long maxRetryTimeoutMillis, Header[] defaultHeaders,
                           HttpHost[] hosts, FailureListener failureListener, CloseableHttpAsyncClient client) {
        super(maxRetryTimeoutMillis, defaultHeaders, hosts, failureListener);
        this.client = client;
        this.client.start();
    }

    public void performRequest(String method, String endpoint, Map<String, String> params,
                               HttpEntity entity, HttpAsyncResponseConsumer<HttpResponse> responseConsumer,
                               ResponseListener responseListener, Header... headers) {
        HttpRequestBase request = createHttpRequest(method, endpoint, params, entity, headers);
        FailureTrackingListener failureTrackingListener = new FailureTrackingListener(responseListener);
        long startTime = System.nanoTime();
        performRequest(startTime, nextHost().iterator(), request, responseConsumer, failureTrackingListener);
    }

    private void performRequest(final long startTime, final Iterator<HttpHost> hosts, final HttpRequestBase request,
                                final HttpAsyncResponseConsumer<HttpResponse> responseConsumer,
                                final FailureTrackingListener listener) {
        final HttpHost host = hosts.next();
        //we stream the request body if the entity allows for it
        HttpAsyncRequestProducer requestProducer = HttpAsyncMethods.create(host, request);

        client.execute(requestProducer, responseConsumer, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                try {
                    RequestLogger.logResponse(logger, request, host, httpResponse);
                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    Response response = new Response(request.getRequestLine(), host, httpResponse);
                    if (isSuccessfulResponse(request.getMethod(), statusCode)) {
                        onResponse(host);
                        listener.onSuccess(response);
                    } else {
                        ResponseException responseException = createResponseException(response);
                        if (mustRetry(statusCode)) {
                            //mark host dead and retry against next one
                            onFailure(host);
                            retryIfPossible(responseException, hosts, request);
                        } else {
                            //mark host alive and don't retry, as the error should be a request problem
                            onResponse(host);
                            listener.onDefinitiveFailure(responseException);
                        }
                    }
                } catch(Exception e) {
                    listener.onDefinitiveFailure(e);
                }
            }

            @Override
            public void failed(Exception failure) {
                try {
                    RequestLogger.logFailedRequest(logger, request, host, failure);
                    onFailure(host);
                    retryIfPossible(failure, hosts, request);
                } catch(Exception e) {
                    listener.onDefinitiveFailure(e);
                }
            }

            private void retryIfPossible(Exception exception, Iterator<HttpHost> hosts, HttpRequestBase request) {
                if (hosts.hasNext()) {
                    //in case we are retrying, check whether maxRetryTimeout has been reached
                    long timeElapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                    long timeout = maxRetryTimeoutMillis - timeElapsedMillis;
                    if (timeout <= 0) {
                        IOException retryTimeoutException = new IOException(
                                "request retries exceeded max retry timeout [" + maxRetryTimeoutMillis + "]");
                        listener.onDefinitiveFailure(retryTimeoutException);
                    } else {
                        listener.trackFailure(exception);
                        request.reset();
                        performRequest(startTime, hosts, request, responseConsumer, listener);
                    }
                } else {
                    listener.onDefinitiveFailure(exception);
                }
            }

            @Override
            public void cancelled() {
            }
        });
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static class FailureTrackingListener {
        private final ResponseListener responseListener;
        private volatile Exception exception;

        FailureTrackingListener(ResponseListener responseListener) {
            this.responseListener = responseListener;
        }

        void onSuccess(Response response) {
            responseListener.onSuccess(response);
        }

        void onDefinitiveFailure(Exception exception) {
            trackFailure(exception);
            responseListener.onFailure(this.exception);
        }

        void trackFailure(Exception exception) {
            this.exception = addSuppressedException(this.exception, exception);
        }
    }

    public interface ResponseListener {

        void onSuccess(Response response);

        void onFailure(Exception exception);
    }

    /**
     * Returns a new {@link Builder} to help with {@link AbstractRestClient} creation.
     */
    public static Builder builder(HttpHost... hosts) {
        return new Builder(hosts);
    }

    public static final class Builder extends AbstractRestClient.Builder {

        private CloseableHttpAsyncClient httpClient;

        /**
         * Creates a new builder instance and sets the hosts that the client will send requests to.
         */
        protected Builder(HttpHost... hosts) {
            super(hosts);
        }

        /**
         * Sets the async http client
         *
         * @see CloseableHttpAsyncClient
         */
        public void setHttpClient(CloseableHttpAsyncClient httpClient) {
            this.httpClient = httpClient;
        }

        /**
         * Creates a {@link CloseableHttpClient} with default settings. Used when the async http client instance is not provided.
         *
         * @see CloseableHttpAsyncClient
         */
        public static CloseableHttpAsyncClient createDefaultHttpClient() {
            return HttpAsyncClientBuilder.create().setDefaultRequestConfig(DEFAULT_REQUEST_CONFIG).build();
        }

        /**
         * Creates a new {@link AbstractRestClient} based on the provided configuration.
         */
        public RestAsyncClient build() {
            if (httpClient == null) {
                httpClient = createDefaultHttpClient();
            }
            if (failureListener == null) {
                failureListener = new FailureListener();
            }
            return new RestAsyncClient(maxRetryTimeout, defaultHeaders, hosts, failureListener, httpClient);
        }
    }
}
