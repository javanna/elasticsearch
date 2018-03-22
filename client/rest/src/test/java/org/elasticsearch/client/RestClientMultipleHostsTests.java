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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.elasticsearch.client.RestClientTestUtil.randomErrorNoRetryStatusCode;
import static org.elasticsearch.client.RestClientTestUtil.randomErrorRetryStatusCode;
import static org.elasticsearch.client.RestClientTestUtil.randomHttpMethod;
import static org.elasticsearch.client.RestClientTestUtil.randomOkStatusCode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RestClient} behaviour against multiple hosts: fail-over, blacklisting etc.
 * Relies on a mock http client to intercept requests and return desired responses based on request path.
 */
public class RestClientMultipleHostsTests extends RestClientTestCase {

    private ExecutorService exec = Executors.newFixedThreadPool(1);
    private RestClient restClient;
    private HttpHost[] httpHosts;
    private HostsTrackingFailureListener failureListener;

    @Before
    @SuppressWarnings("unchecked")
    public void createRestClient() {
        CloseableHttpAsyncClient httpClient = mock(CloseableHttpAsyncClient.class);
        when(httpClient.<HttpResponse>execute(any(HttpAsyncRequestProducer.class), any(HttpAsyncResponseConsumer.class),
               any(HttpClientContext.class), any(FutureCallback.class))).thenAnswer(new Answer<Future<HttpResponse>>() {
            @Override
            public Future<HttpResponse> answer(InvocationOnMock invocationOnMock) throws Throwable {
                HttpAsyncRequestProducer requestProducer = (HttpAsyncRequestProducer) invocationOnMock.getArguments()[0];
                final HttpUriRequest request = (HttpUriRequest)requestProducer.generateRequest();
                final HttpHost httpHost = requestProducer.getTarget();
                HttpClientContext context = (HttpClientContext) invocationOnMock.getArguments()[2];
                assertThat(context.getAuthCache().get(httpHost), instanceOf(BasicScheme.class));
                final FutureCallback<HttpResponse> futureCallback = (FutureCallback<HttpResponse>) invocationOnMock.getArguments()[3];
                //return the desired status code or exception depending on the path
                exec.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (request.getURI().getPath().equals("/soe")) {
                            futureCallback.failed(new SocketTimeoutException(httpHost.toString()));
                        } else if (request.getURI().getPath().equals("/coe")) {
                            futureCallback.failed(new ConnectTimeoutException(httpHost.toString()));
                        } else if (request.getURI().getPath().equals("/ioe")) {
                            futureCallback.failed(new IOException(httpHost.toString()));
                        } else {
                            int statusCode = Integer.parseInt(request.getURI().getPath().substring(1));
                            StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("http", 1, 1), statusCode, "");
                            futureCallback.completed(new BasicHttpResponse(statusLine));
                        }
                    }
                });
                return null;
            }
        });
        int numHosts = RandomNumbers.randomIntBetween(getRandom(), 2, 5);
        httpHosts = new HttpHost[numHosts];
        for (int i = 0; i < numHosts; i++) {
            httpHosts[i] = new HttpHost("localhost", 9200 + i);
        }
        failureListener = new HostsTrackingFailureListener();
        restClient = new RestClient(httpClient, RequestConfig.DEFAULT, 10000, new Header[0], httpHosts, null, failureListener);
    }

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        exec.shutdown();
    }

    public void testRoundRobinOkStatusCodes() throws IOException {
        int numIters = RandomNumbers.randomIntBetween(getRandom(), 1, 5);
        for (int i = 0; i < numIters; i++) {
            for (int j = 0; j < httpHosts.length; j++) {
                int statusCode = randomOkStatusCode(getRandom());
                Response response = restClient.performRequest(randomHttpMethod(getRandom()), "/" + statusCode);
                assertEquals(statusCode, response.getStatusLine().getStatusCode());
                assertEquals(httpHosts[j], response.getHost());
            }
        }
        failureListener.assertNotCalled();
    }

    public void testRoundRobinNoRetryErrors() throws IOException {
        int numIters = RandomNumbers.randomIntBetween(getRandom(), 1, 5);
        for (int i = 0; i < numIters; i++) {
            for (int j = 0; j < httpHosts.length; j++) {
                String method = randomHttpMethod(getRandom());
                int statusCode = randomErrorNoRetryStatusCode(getRandom());
                try {
                    Response response = restClient.performRequest(method, "/" + statusCode);
                    if (method.equals("HEAD") && statusCode == 404) {
                        //no exception gets thrown although we got a 404
                        assertEquals(404, response.getStatusLine().getStatusCode());
                        assertEquals(statusCode, response.getStatusLine().getStatusCode());
                        assertEquals(httpHosts[j], response.getHost());
                    } else {
                        fail("request should have failed");
                    }
                } catch (ResponseException e) {
                    if (method.equals("HEAD") && statusCode == 404) {
                        throw e;
                    }
                    Response response = e.getResponse();
                    assertEquals(statusCode, response.getStatusLine().getStatusCode());
                    assertEquals(httpHosts[j], response.getHost());
                    assertEquals(0, e.getSuppressed().length);
                }
            }
        }
        failureListener.assertNotCalled();
    }

    //TODO this one fails, rewrite!
    public void RoundRobinRetryErrors() throws IOException {
        String retryEndpoint = randomErrorRetryEndpoint();
        try  {
            restClient.performRequest(randomHttpMethod(getRandom()), retryEndpoint);
            fail("request should have failed");
        } catch (ResponseException e) {
            /*
             * Unwrap the top level failure that was added so the stack trace contains
             * the caller. It wraps the exception that contains the failed hosts.
             */
            e = (ResponseException) e.getCause();
            //first request causes all the hosts to be blacklisted, the returned exception holds one suppressed exception each
            failureListener.assertCalled(httpHosts);
            for (int i = httpHosts.length - 1; i >= 0; i--) {
                Response response = e.getResponse();
                assertEquals(Integer.parseInt(retryEndpoint.substring(1)), response.getStatusLine().getStatusCode());
                assertEquals(httpHosts[i], response.getHost());
                if (i == 0) {
                    assertEquals(0, e.getSuppressed().length);
                } else {
                    assertEquals(1, e.getSuppressed().length);
                    Throwable suppressed = e.getSuppressed()[0];
                    assertThat(suppressed, instanceOf(ResponseException.class));
                    e = (ResponseException)suppressed;
                }
            }
        } catch (IOException e) {
            /*
             * Unwrap the top level failure that was added so the stack trace contains
             * the caller. It wraps the exception that contains the failed hosts.
             */
            e = (IOException) e.getCause();
            //first request causes all the hosts to be blacklisted, the returned exception holds one suppressed exception each
            failureListener.assertCalled(httpHosts);
            for (int i = httpHosts.length - 1; i >= 0; i--) {
                 assertEquals(httpHosts[i], HttpHost.create(e.getMessage()));
                if (i == 0) {
                    assertEquals(0, e.getSuppressed().length);
                } else {
                    assertEquals(1, e.getSuppressed().length);
                    Throwable suppressed = e.getSuppressed()[0];
                    assertThat(suppressed, instanceOf(IOException.class));
                    e = (IOException)suppressed;
                }
            }
        }

        int numIters = RandomNumbers.randomIntBetween(getRandom(), 3, 10);
        int currentHostIndex = 0;
        int cumulativeErrorRetryIters = 0;
        for (int i = 1; i <= numIters; i++) {
            int errorRetryIters = randomIntBetween(1, 5);
            cumulativeErrorRetryIters += errorRetryIters;
            for (int j = 0; j < errorRetryIters; j++) {
                for (int k = 0; k < httpHosts.length; k++) {
                    retryEndpoint = randomErrorRetryEndpoint();
                    try {
                        restClient.performRequest(randomHttpMethod(getRandom()), retryEndpoint);
                        fail("request should have failed");
                    } catch (ResponseException e) {
                        Response response = e.getResponse();
                        assertThat(response.getStatusLine().getStatusCode(), equalTo(Integer.parseInt(retryEndpoint.substring(1))));
                        assertEquals(httpHosts[currentHostIndex % httpHosts.length], response.getHost());
                        //after the first request, all hosts are blacklisted, a single one gets resurrected each time
                        failureListener.assertCalled(response.getHost());
                        assertEquals(0, e.getSuppressed().length);
                    } catch (IOException e) {
                        /*
                         * Unwrap the top level failure that was added so the stack trace contains
                         * the caller. It wraps the exception that contains the failed hosts.
                         */
                        e = (IOException) e.getCause();
                        HttpHost httpHost = HttpHost.create(e.getMessage());
                        assertEquals(httpHosts[currentHostIndex % httpHosts.length], httpHost);
                        //after the first request, all hosts are blacklisted, a single one gets resurrected each time
                        failureListener.assertCalled(httpHost);
                        assertEquals(0, e.getSuppressed().length);
                    }
                    currentHostIndex++;
                }
            }
            if (getRandom().nextBoolean()) {
                int currentHost = currentHostIndex % httpHosts.length; //TODO this is always 0!
                //mark one host back alive through a successful request and check that all requests after that are sent to it
                int errorNoRetryIters = RandomNumbers.randomIntBetween(getRandom(), 1, 10);
                for (int y = 0; y < errorNoRetryIters; y++) {
                    int statusCode = randomErrorNoRetryStatusCode(getRandom());
                    Response response;
                    try {
                        response = restClient.performRequest(randomHttpMethod(getRandom()), "/" + statusCode);
                    } catch (ResponseException e) {
                        response = e.getResponse();
                    }
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(statusCode));
                    assertEquals(httpHosts[currentHost], response.getHost());
                }
                failureListener.assertNotCalled();
                //let the selected host catch up on number of failures, it gets selected a consecutive number of times as it's the one
                //selected to be retried earlier (due to lower number of failures) till all the hosts have the same number of failures
                for (int y = 0; y <= cumulativeErrorRetryIters; y++) {
                    retryEndpoint = randomErrorRetryEndpoint();
                    try {
                        restClient.performRequest(randomHttpMethod(getRandom()), retryEndpoint);
                        fail("request should have failed");
                    } catch (ResponseException e) {
                        Response response = e.getResponse();
                        assertThat(response.getStatusLine().getStatusCode(), equalTo(Integer.parseInt(retryEndpoint.substring(1))));
                        assertEquals(httpHosts[currentHost], response.getHost());
                        failureListener.assertCalled(response.getHost());
                    } catch(IOException e) {
                        /*
                         * Unwrap the top level failure that was added so the stack trace contains
                         * the caller. It wraps the exception that contains the failed hosts.
                         */
                        e = (IOException) e.getCause();
                        HttpHost httpHost = HttpHost.create(e.getMessage());
                        assertEquals(httpHosts[currentHost], httpHost);
                        failureListener.assertCalled(httpHost);
                    }
                }
                currentHostIndex++;
            }

        }
    }

    private static String randomErrorRetryEndpoint() {
        switch(RandomNumbers.randomIntBetween(getRandom(), 0, 3)) {
            case 0:
                return "/" + randomErrorRetryStatusCode(getRandom());
            case 1:
                return "/coe";
            case 2:
                return "/soe";
            case 3:
                return "/ioe";
        }
        throw new UnsupportedOperationException();
    }

    //TODO open issue for immediate retries at the first failure for each host

    //TODO highlight consistent ordering for hosts

    //TODO test that there are no nodes that stay in the blacklist forever

    //TODO multi-threading test (take the one from Nik?)
}
