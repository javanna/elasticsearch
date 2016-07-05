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
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractRestClient implements Closeable {
    private static final Log logger = LogFactory.getLog(AbstractRestClient.class);
    public static ContentType JSON_CONTENT_TYPE = ContentType.create("application/json", Consts.UTF_8);

    //we don't rely on default headers supported by HttpClient as those cannot be replaced, plus it would get hairy
    //when we create the HttpClient instance on our own as there would be two different ways to set the default headers.
    private final Header[] defaultHeaders;
    protected final long maxRetryTimeoutMillis;
    private final AtomicInteger lastHostIndex = new AtomicInteger(0);
    private volatile Set<HttpHost> hosts;
    private final ConcurrentMap<HttpHost, DeadHostState> blacklist = new ConcurrentHashMap<>();
    private final FailureListener failureListener;

    protected AbstractRestClient(long maxRetryTimeoutMillis, Header[] defaultHeaders,
                                 HttpHost[] hosts, FailureListener failureListener) {
        this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
        this.defaultHeaders = defaultHeaders;
        this.failureListener = failureListener;
        setHosts(hosts);
    }

    /**
     * Replaces the hosts that the client communicates with.
     * @see HttpHost
     */
    public synchronized void setHosts(HttpHost... hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts must not be null nor empty");
        }
        Set<HttpHost> httpHosts = new HashSet<>();
        for (HttpHost host : hosts) {
            Objects.requireNonNull(host, "host cannot be null");
            httpHosts.add(host);
        }
        this.hosts = Collections.unmodifiableSet(httpHosts);
        this.blacklist.clear();
    }

    protected final HttpRequestBase createHttpRequest(String method, String endpoint, Map<String, String> params,
                                                      HttpEntity entity, Header... headers) {
        URI uri = buildUri(endpoint, params);
        HttpRequestBase request = createHttpRequest(method, uri, entity);
        setHeaders(request, headers);
        return request;
    }

    /**
     * Sets the headers to the provided request, default headers with same name as request headers will be overwritten
     */
    protected final void setHeaders(HttpRequest httpRequest, Header[] requestHeaders) {
        Objects.requireNonNull(requestHeaders, "request headers must not be null");
        for (Header defaultHeader : defaultHeaders) {
            httpRequest.setHeader(defaultHeader);
        }
        for (Header requestHeader : requestHeaders) {
            Objects.requireNonNull(requestHeader, "request header must not be null");
            httpRequest.setHeader(requestHeader);
        }
    }

    /**
     * Returns an iterator of hosts to be used for a request call.
     * Ideally, the first host is retrieved from the iterator and used successfully for the request.
     * Otherwise, after each failure the next host should be retrieved from the iterator so that the request can be retried till
     * the iterator is exhausted. The maximum total of attempts is equal to the number of hosts that are available in the iterator.
     * The iterator returned will never be empty, rather an {@link IllegalStateException} in case there are no hosts.
     * In case there are no healthy hosts available, or dead ones to be be retried, one dead host gets returned.
     */
    protected final Iterable<HttpHost> nextHost() {
        Set<HttpHost> filteredHosts = new HashSet<>(hosts);
        for (Map.Entry<HttpHost, DeadHostState> entry : blacklist.entrySet()) {
            if (System.nanoTime() - entry.getValue().getDeadUntilNanos() < 0) {
                filteredHosts.remove(entry.getKey());
            }
        }

        if (filteredHosts.isEmpty()) {
            //last resort: if there are no good hosts to use, return a single dead one, the one that's closest to being retried
            List<Map.Entry<HttpHost, DeadHostState>> sortedHosts = new ArrayList<>(blacklist.entrySet());
            Collections.sort(sortedHosts, new Comparator<Map.Entry<HttpHost, DeadHostState>>() {
                @Override
                public int compare(Map.Entry<HttpHost, DeadHostState> o1, Map.Entry<HttpHost, DeadHostState> o2) {
                    return Long.compare(o1.getValue().getDeadUntilNanos(), o2.getValue().getDeadUntilNanos());
                }
            });
            HttpHost deadHost = sortedHosts.get(0).getKey();
            logger.trace("resurrecting host [" + deadHost + "]");
            return Collections.singleton(deadHost);
        }

        List<HttpHost> rotatedHosts = new ArrayList<>(filteredHosts);
        Collections.rotate(rotatedHosts, rotatedHosts.size() - lastHostIndex.getAndIncrement());
        return rotatedHosts;
    }

    protected final boolean isSuccessfulResponse(String method, int statusCode) {
        return statusCode < 300 || (HttpHead.METHOD_NAME.equals(method) && statusCode == 404);
    }

    protected final boolean mustRetry(int statusCode) {
        switch(statusCode) {
            case 502:
            case 503:
            case 504:
                return true;
        }
        return false;
    }

    protected final ResponseException createResponseException(Response response) {
        String responseBody = null;
        try {
            if (response.getEntity() != null) {
                responseBody = EntityUtils.toString(response.getEntity());
            }
        } catch (IOException e) {
            logger.warn("error while reading response body", e);
        } finally {
            try {
                response.close();
            } catch (IOException e) {
                logger.warn("error while closing response object", e);
            }
        }
        return new ResponseException(response, responseBody);
    }

    /**
     * Called after each successful request call.
     * Receives as an argument the host that was used for the successful request.
     */
    protected final void onResponse(HttpHost host) {
        DeadHostState removedHost = this.blacklist.remove(host);
        if (logger.isDebugEnabled() && removedHost != null) {
            logger.debug("removed host [" + host + "] from blacklist");
        }
    }

    /**
     * Called after each failed attempt.
     * Receives as an argument the host that was used for the failed attempt.
     */
    protected final void onFailure(HttpHost host) {
        while(true) {
            DeadHostState previousDeadHostState = blacklist.putIfAbsent(host, DeadHostState.INITIAL_DEAD_STATE);
            if (previousDeadHostState == null) {
                logger.debug("added host [" + host + "] to blacklist");
                break;
            }
            if (blacklist.replace(host, previousDeadHostState, new DeadHostState(previousDeadHostState))) {
                logger.debug("updated host [" + host + "] already in blacklist");
                break;
            }
        }
        failureListener.onFailure(host);
    }

    protected static <E extends Exception> E addSuppressedException(E suppressedException, E currentException) {
        if (suppressedException != null) {
            currentException.addSuppressed(suppressedException);
        }
        return currentException;
    }

    protected static HttpRequestBase createHttpRequest(String method, URI uri, HttpEntity entity) {
        switch(method.toUpperCase(Locale.ROOT)) {
            case HttpDeleteWithEntity.METHOD_NAME:
                return addRequestBody(new HttpDeleteWithEntity(uri), entity);
            case HttpGetWithEntity.METHOD_NAME:
                return addRequestBody(new HttpGetWithEntity(uri), entity);
            case HttpHead.METHOD_NAME:
                return addRequestBody(new HttpHead(uri), entity);
            case HttpOptions.METHOD_NAME:
                return addRequestBody(new HttpOptions(uri), entity);
            case HttpPatch.METHOD_NAME:
                return addRequestBody(new HttpPatch(uri), entity);
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(uri);
                addRequestBody(httpPost, entity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                return addRequestBody(new HttpPut(uri), entity);
            case HttpTrace.METHOD_NAME:
                return addRequestBody(new HttpTrace(uri), entity);
            default:
                throw new UnsupportedOperationException("http method not supported: " + method);
        }
    }

    protected static HttpRequestBase addRequestBody(HttpRequestBase httpRequest, HttpEntity entity) {
        if (entity != null) {
            if (httpRequest instanceof HttpEntityEnclosingRequestBase) {
                ((HttpEntityEnclosingRequestBase)httpRequest).setEntity(entity);
            } else {
                throw new UnsupportedOperationException(httpRequest.getMethod() + " with body is not supported");
            }
        }
        return httpRequest;
    }

    protected static URI buildUri(String path, Map<String, String> params) {
        Objects.requireNonNull(params, "params must not be null");
        try {
            URIBuilder uriBuilder = new URIBuilder(path);
            for (Map.Entry<String, String> param : params.entrySet()) {
                uriBuilder.addParameter(param.getKey(), param.getValue());
            }
            return uriBuilder.build();
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Rest client builder. Helps creating a new {@link AbstractRestClient}.
     */
    public abstract static class Builder {
        public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 1000;
        public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 10000;
        public static final int DEFAULT_MAX_RETRY_TIMEOUT_MILLIS = DEFAULT_SOCKET_TIMEOUT_MILLIS;
        public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT_MILLIS = 500;
        //default timeouts are all infinite
        public static final RequestConfig DEFAULT_REQUEST_CONFIG = RequestConfig.custom()
                .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_MILLIS)
                .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT_MILLIS)
                .setConnectionRequestTimeout(DEFAULT_CONNECTION_REQUEST_TIMEOUT_MILLIS).build();

        private static final Header[] EMPTY_HEADERS = new Header[0];

        protected final HttpHost[] hosts;
        protected int maxRetryTimeout = DEFAULT_MAX_RETRY_TIMEOUT_MILLIS;
        protected Header[] defaultHeaders = EMPTY_HEADERS;
        protected FailureListener failureListener;

        /**
         * Creates a new builder instance and sets the hosts that the client will send requests to.
         */
        protected Builder(HttpHost... hosts) {
            if (hosts == null || hosts.length == 0) {
                throw new IllegalArgumentException("no hosts provided");
            }
            this.hosts = hosts;
        }

        /**
         * Sets the maximum timeout (in milliseconds) to honour in case of multiple retries of the same request.
         * {@link #DEFAULT_MAX_RETRY_TIMEOUT_MILLIS} if not specified.
         *
         * @throws IllegalArgumentException if maxRetryTimeoutMillis is not greater than 0
         */
        public final void setMaxRetryTimeoutMillis(int maxRetryTimeoutMillis) {
            if (maxRetryTimeoutMillis <= 0) {
                throw new IllegalArgumentException("maxRetryTimeoutMillis must be greater than 0");
            }
            this.maxRetryTimeout = maxRetryTimeoutMillis;
        }

        /**
         * Sets the default request headers, which will be sent with every request
         */
        public final void setDefaultHeaders(Header[] defaultHeaders) {
            Objects.requireNonNull(defaultHeaders, "default headers must not be null");
            for (Header defaultHeader : defaultHeaders) {
                Objects.requireNonNull(defaultHeader, "default header must not be null");
            }
            this.defaultHeaders = defaultHeaders;
        }

        /**
         * Sets the {@link FailureListener} to be notified for each request failure
         */
        public final void setFailureListener(FailureListener failureListener) {
            Objects.requireNonNull(failureListener, "failure listener must not be null");
            this.failureListener = failureListener;
        }
    }

    /**
     * Listener that allows to be notified whenever a failure happens. Useful when sniffing is enabled, so that we can sniff on failure.
     * The default implementation is a no-op.
     */
    public static class FailureListener {
        /**
         * Notifies that the host provided as argument has just failed
         */
        public void onFailure(HttpHost host) {

        }
    }
}
