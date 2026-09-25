/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

/**
 * Floods a heap-constrained coordinating-only node with concurrent knn
 * searches. knn always uses {@code dfs_query_then_fetch}, so the coordinator
 * holds an extra DFS/knn merge phase compared to
 * {@link CoordinatorOverloadQueryThenFetchIT}. The coordinator must stay up.
 * Today it OOMs, so this fails until that is fixed.
 */
public class CoordinatorOverloadKnnDfsIT extends CoordinatorOverloadTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = buildCluster();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    public void testCoordinatorSurvivesKnnDfsFlood() throws Exception {
        floodAndAssertCoordinatorSurvives(() -> {
            Request search = new Request("POST", "/" + INDEX + "/_search");
            search.setJsonEntity(knnSearchBody(SEARCH_SIZE, SEARCH_SIZE));
            return search;
        }, KNN_CONCURRENCY);
    }
}
