/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Shared helpers for coordinator-overload tests. Subclasses each own a
 * {@code @ClassRule} cluster so a coordinator OOM in one search type cannot
 * poison the other.
 *
 * <p>Production hits this on shared-role nodes. The lab cluster still splits
 * roles and starves only the coordinator so a death is isolated.
 *
 * <p>The flood must not kill the coordinator. Today it does (OOM); the tests
 * fail until admission control or an equivalent fix keeps the node up.
 */
public abstract class CoordinatorOverloadTestCase extends ESRestTestCase {

    protected static final String INDEX = "coord-overload";
    protected static final int COORDINATOR_NODE = 0;
    protected static final int SHARDS = 5;
    protected static final int DOCS = 200;
    protected static final int DIMS = 8;
    protected static final int CONCURRENCY = 256;
    /** knn/DFS needs more in-flight searches than QTF to push a 96 MB coordinator over. */
    protected static final int KNN_CONCURRENCY = 512;
    protected static final int SEARCH_SIZE = 200;
    private static final int SOCKET_TIMEOUT_MINUTES = 2;
    // Long enough that today's unrestricted flood OOMs a 96 MB coordinator (~10s).
    private static final int FLOOD_SECONDS = 30;

    private RestClient coordinatorClient;
    private String dataNodeHttpAddress;

    /**
     * Builds the split-role cluster: a 96 MB coordinating-only node and a
     * 512 MB master+data node whose search pool is a single thread.
     */
    protected static ElasticsearchCluster buildCluster() {
        return ElasticsearchCluster.local()
            .withNode(
                node -> node.name("coordinator")
                    .setting("node.roles", "[]")
                    .jvmArg("-Xms96m")
                    .jvmArg("-Xmx96m")
                    .jvmArg("-XX:+ExitOnOutOfMemoryError")
                    .jvmArg("-XX:-HeapDumpOnOutOfMemoryError")
            )
            .withNode(node -> node.name("data").setting("node.roles", "[master, data]").setting("thread_pool.search.size", "1"))
            .build();
    }

    protected abstract ElasticsearchCluster cluster();

    @Override
    protected String getTestRestCluster() {
        // Both nodes: getHttpAddress(index) is not ordered (parallelStream).
        return cluster().getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean shouldFailureSkipRemainingTests() {
        return true;
    }

    @Before
    public void setUpIndexAndCoordinatorClient() throws IOException {
        if (indexExists(INDEX) == false) {
            createIndex(
                client(),
                INDEX,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARDS)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build(),
                """
                    {
                      "properties": {
                        "tag": { "type": "keyword" },
                        "vector": {
                          "type": "dense_vector",
                          "dims": 8,
                          "index": true,
                          "similarity": "l2_norm"
                        }
                      }
                    }
                    """
            );
            Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity(bulkBody());
            assertOK(client().performRequest(bulk));
        }
        if (coordinatorClient == null) {
            dataNodeHttpAddress = httpAddressForNode("data");
            coordinatorClient = buildCoordinatorClient();
        }
    }

    @After
    public void closeCoordinatorClient() throws IOException {
        if (coordinatorClient != null) {
            coordinatorClient.close();
            coordinatorClient = null;
        }
    }

    /**
     * Keeps {@link #CONCURRENCY} searches in flight against the coordinator
     * for {@link #FLOOD_SECONDS}, then asserts that JVM is still alive and
     * both nodes still serve HTTP. Fails if the coordinator OOMs.
     */
    protected void floodAndAssertCoordinatorSurvives(Supplier<Request> requestFactory) throws Exception {
        floodAndAssertCoordinatorSurvives(requestFactory, CONCURRENCY);
    }

    protected void floodAndAssertCoordinatorSurvives(Supplier<Request> requestFactory, int concurrency) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        try {
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < concurrency; i++) {
                pool.submit(() -> {
                    start.await();
                    while (Thread.currentThread().isInterrupted() == false) {
                        try {
                            EntityUtils.consumeQuietly(coordinatorClient.performRequest(requestFactory.get()).getEntity());
                        } catch (ResponseException e) {
                            EntityUtils.consumeQuietly(e.getResponse().getEntity());
                        } catch (Exception e) {
                            // Connection errors if the coordinator dies under load.
                            return null;
                        }
                    }
                    return null;
                });
            }
            start.countDown();
            TimeUnit.SECONDS.sleep(FLOOD_SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertCoordinatorSurvived();
    }

    protected static String knnSearchBody(int k, int numCandidates) {
        return String.format(
            Locale.ROOT,
            "{\"knn\":{\"field\":\"vector\",\"query_vector\":%s,\"k\":%d,\"num_candidates\":%d},\"size\":%d}",
            queryVectorJson(),
            k,
            numCandidates,
            k
        );
    }

    private void assertCoordinatorSurvived() throws IOException {
        assertTrue("coordinator JVM exited during the search flood (likely OOM)", coordinatorIsAlive());
        assertOK(coordinatorClient.performRequest(new Request("GET", "/_cluster/health")));
        try (RestClient dataClient = RestClient.builder(parseClusterHosts(dataNodeHttpAddress).toArray(HttpHost[]::new)).build()) {
            assertOK(dataClient.performRequest(new Request("GET", "/_cluster/health")));
        }
    }

    private boolean coordinatorIsAlive() {
        try {
            return ProcessHandle.of(cluster().getPid(COORDINATOR_NODE)).map(ProcessHandle::isAlive).orElse(false);
        } catch (RuntimeException e) {
            logger.info("could not read coordinator pid", e);
            return false;
        }
    }

    private RestClient buildCoordinatorClient() throws IOException {
        List<HttpHost> hosts = parseClusterHosts(httpAddressForNode("coordinator"));
        return RestClient.builder(hosts.toArray(HttpHost[]::new))
            .setRequestConfigCallback(builder -> builder.setSocketTimeout((int) TimeUnit.MINUTES.toMillis(SOCKET_TIMEOUT_MINUTES)))
            .setHttpClientConfigCallback(
                httpClientBuilder -> httpClientBuilder.setMaxConnPerRoute(KNN_CONCURRENCY).setMaxConnTotal(KNN_CONCURRENCY)
            )
            .build();
    }

    private String httpAddressForNode(String nodeName) throws IOException {
        Request request = new Request("GET", "/_cat/nodes");
        request.addParameter("format", "json");
        request.addParameter("h", "name,http");
        for (Object row : entityAsList(client().performRequest(request))) {
            Map<?, ?> node = (Map<?, ?>) row;
            if (nodeName.equals(node.get("name"))) {
                return node.get("http").toString();
            }
        }
        throw new AssertionError("no HTTP address for node [" + nodeName + "]");
    }

    private static String bulkBody() {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOCS; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"tag\":\"").append(i).append("\",\"vector\":").append(vectorJson(i)).append("}\n");
        }
        return bulk.toString();
    }

    private static String vectorJson(int seed) {
        StringBuilder vector = new StringBuilder("[");
        for (int d = 0; d < DIMS; d++) {
            if (d > 0) {
                vector.append(',');
            }
            vector.append((float) (seed + d));
        }
        return vector.append(']').toString();
    }

    private static String queryVectorJson() {
        return vectorJson(0);
    }
}
