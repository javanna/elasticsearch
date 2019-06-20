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

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public final class SearchPhasesExecutor {

    private static final Logger logger = Loggers.getLogger(SearchPhasesExecutor.class);

    private final ThreadPool threadPool;
    private final SearchPhaseController searchPhaseController;
    private final SearchTransportService searchTransportService;

    SearchPhasesExecutor(ThreadPool threadPool, SearchPhaseController searchPhaseController, SearchTransportService searchTransportService) {
        this.threadPool = threadPool;
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
    }

    //TODO all these arguments, or at least most of them, could be held by a request type of object
    public void execute(SearchTask task,
                        SearchRequest searchRequest,
                        GroupShardsIterator<SearchShardIterator> shardIterators,
                        TransportSearchAction.SearchTimeProvider timeProvider,
                        BiFunction<String, String, Transport.Connection> connectionLookup,
                        long clusterStateVersion,
                        Map<String, AliasFilter> aliasFilter,
                        Map<String, Float> concreteIndexBoosts,
                        Map<String, Set<String>> indexRoutings,
                        ActionListener<SearchResponse> listener,
                        boolean preFilterSearchShards,
                        SearchResponse.Clusters clusters) {
        searchAsyncAction(task, searchRequest, shardIterators, timeProvider, connectionLookup, clusterStateVersion,
            Collections.unmodifiableMap(aliasFilter), concreteIndexBoosts, indexRoutings, listener, preFilterSearchShards,
            clusters).start();
    }

    private AbstractSearchAsyncAction searchAsyncAction(SearchTask task, SearchRequest searchRequest,
                                                        GroupShardsIterator<SearchShardIterator> shardIterators,
                                                        TransportSearchAction.SearchTimeProvider timeProvider,
                                                        BiFunction<String, String, Transport.Connection> connectionLookup,
                                                        long clusterStateVersion,
                                                        Map<String, AliasFilter> aliasFilter,
                                                        Map<String, Float> concreteIndexBoosts,
                                                        Map<String, Set<String>> indexRoutings,
                                                        ActionListener<SearchResponse> listener,
                                                        boolean preFilter,
                                                        SearchResponse.Clusters clusters) {
        Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
        if (preFilter) {
            return new CanMatchPreFilterSearchPhase(logger, searchTransportService, connectionLookup,
                aliasFilter, concreteIndexBoosts, indexRoutings, executor, searchRequest, listener, shardIterators,
                timeProvider, clusterStateVersion, task, (iter) -> {
                AbstractSearchAsyncAction action = searchAsyncAction(task, searchRequest, iter, timeProvider, connectionLookup,
                    clusterStateVersion, aliasFilter, concreteIndexBoosts, indexRoutings, listener, false, clusters);
                return new SearchPhase(action.getName()) {
                    @Override
                    public void run() {
                        action.start();
                    }
                };
            }, clusters);
        } else {
            AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction;
            switch (searchRequest.searchType()) {
                case DFS_QUERY_THEN_FETCH:
                    searchAsyncAction = new SearchDfsQueryThenFetchAsyncAction(logger, searchTransportService, connectionLookup,
                        aliasFilter, concreteIndexBoosts, indexRoutings, searchPhaseController, executor, searchRequest, listener,
                        shardIterators, timeProvider, clusterStateVersion, task, clusters);
                    break;
                case QUERY_THEN_FETCH:
                    searchAsyncAction = new SearchQueryThenFetchAsyncAction(logger, searchTransportService, connectionLookup,
                        aliasFilter, concreteIndexBoosts, indexRoutings, searchPhaseController, executor, searchRequest, listener,
                        shardIterators, timeProvider, clusterStateVersion, task, clusters);
                    break;
                default:
                    throw new IllegalStateException("Unknown search type: [" + searchRequest.searchType() + "]");
            }
            return searchAsyncAction;
        }
    }
}
