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
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Client that connects to an elasticsearch cluster through http.
 * Must be created using {@link Builder}, which allows to set all the different options or just rely on defaults.
 * The hosts that are part of the cluster need to be provided at creation time, but can also be replaced later
 * by calling {@link #setHosts(HttpHost...)}.
 * The method {@link #performRequest(String, String, Map, HttpEntity, Header...)} allows to send a request to the cluster. When
 * sending a request, a host gets selected out of the provided ones in a round-robin fashion. Failing hosts are marked dead and
 * retried after a certain amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously
 * failed (the more failures, the later they will be retried). In case of failures all of the alive nodes (or dead nodes that
 * deserve a retry) are retried till one responds or none of them does, in which case an {@link IOException} will be thrown.
 *
 * Requests can be traced by enabling trace logging for "tracer". The trace logger outputs requests and responses in curl format.
 */

public final class RestClient extends AbstractRestClient {

    private static final Log logger = LogFactory.getLog(RestClient.class);

    private final CloseableHttpClient client;

    private RestClient(long maxRetryTimeoutMillis, Header[] defaultHeaders,
                       HttpHost[] hosts, FailureListener failureListener, CloseableHttpClient client) {
        super(maxRetryTimeoutMillis, defaultHeaders, hosts, failureListener);
        this.client = client;
    }

    /**
     * Sends a request to the elasticsearch cluster that the current client points to.
     * Selects a host out of the provided ones in a round-robin fashion. Failing hosts are marked dead and retried after a certain
     * amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously failed (the more failures,
     * the later they will be retried). In case of failures all of the alive nodes (or dead nodes that deserve a retry) are retried
     * till one responds or none of them does, in which case an {@link IOException} will be thrown.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param headers the optional request headers
     * @return the response returned by elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case elasticsearch responded with a status code that indicated an error
     */
    public Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, Header... headers) throws IOException {
        HttpRequestBase request = createHttpRequest(method, endpoint, params, entity, headers);
        IOException lastSeenException = null;
        long startTime = System.nanoTime();
        for (HttpHost host : nextHost()) {
            if (lastSeenException != null) {
                //in case we are retrying, check whether maxRetryTimeout has been reached, in which case an exception will be thrown
                long timeElapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                long timeout = maxRetryTimeoutMillis - timeElapsedMillis;
                if (timeout <= 0) {
                    IOException retryTimeoutException = new IOException(
                            "request retries exceeded max retry timeout [" + maxRetryTimeoutMillis + "]");
                    retryTimeoutException.addSuppressed(lastSeenException);
                    throw retryTimeoutException;
                }
                //also reset the request to make it reusable for the next attempt
                request.reset();
            }

            CloseableHttpResponse httpResponse;
            try {
                httpResponse = client.execute(host, request);
            } catch(IOException e) {
                RequestLogger.logFailedRequest(logger, request, host, e);
                onFailure(host);
                lastSeenException = addSuppressedException(lastSeenException, e);
                continue;
            }
            RequestLogger.logResponse(logger, request, host, httpResponse);
            Response response = new Response(request.getRequestLine(), host, httpResponse);
            int statusCode = response.getStatusLine().getStatusCode();
            if (isSuccessfulResponse(request.getMethod(), statusCode)) {
                onResponse(host);
                return response;
            }
            ResponseException responseException = createResponseException(response);
            lastSeenException = addSuppressedException(lastSeenException, responseException);
            if (mustRetry(statusCode)) {
                //mark host dead and retry against next one
                onFailure(host);
            } else {
                //mark host alive and don't retry, as the error should be a request problem
                onResponse(host);
                throw lastSeenException;
            }
        }
        //we get here only when we tried all nodes and they all failed
        assert lastSeenException != null;
        throw lastSeenException;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    /**
     * Returns a new {@link Builder} to help with {@link AbstractRestClient} creation.
     */
    public static Builder builder(HttpHost... hosts) {
        return new Builder(hosts);
    }

    public static final class Builder extends AbstractRestClient.Builder {

        private CloseableHttpClient httpClient;

        /**
         * Creates a new builder instance and sets the hosts that the client will send requests to.
         */
        protected Builder(HttpHost... hosts) {
            super(hosts);
        }

        /**
         * Sets the http client. A new default one will be created if not
         * specified, by calling {@link #createDefaultHttpClient(Registry)})}.
         *
         * @see CloseableHttpClient
         */
        public void setHttpClient(CloseableHttpClient httpClient) {
            this.httpClient = httpClient;
        }

        /**
         * Creates a {@link CloseableHttpClient} with default settings. Used when the http client instance is not provided.
         *
         * @see CloseableHttpClient
         */
        public static CloseableHttpClient createDefaultHttpClient(Registry<ConnectionSocketFactory> socketFactoryRegistry) {
            PoolingHttpClientConnectionManager connectionManager;
            if (socketFactoryRegistry == null) {
                connectionManager = new PoolingHttpClientConnectionManager();
            } else {
                connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            }
            //default settings may be too constraining
            connectionManager.setDefaultMaxPerRoute(10);
            connectionManager.setMaxTotal(30);
            return HttpClientBuilder.create().setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(DEFAULT_REQUEST_CONFIG).build();
        }

        /**
         * Creates a new {@link AbstractRestClient} based on the provided configuration.
         */
        public RestClient build() {
            if (httpClient == null) {
                httpClient = createDefaultHttpClient(null);
            }
            if (failureListener == null) {
                failureListener = new FailureListener();
            }
            return new RestClient(maxRetryTimeout, defaultHeaders, hosts, failureListener, httpClient);
        }
    }
}
