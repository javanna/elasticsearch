/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.AbstractSearchAsyncAction;
import org.elasticsearch.action.search.SearchDfsQueryThenFetchAsyncAction;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public class TransportBackgroundSearchAction extends TransportAction<BackgroundSearchRequest, BackgroundSearchResponse> {

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final SearchPhaseController searchPhaseController;
    private final ClusterService clusterService;

    @Inject
    public TransportBackgroundSearchAction(ActionFilters actionFilters, ThreadPool threadPool,
                                           TransportService transportService, SearchTransportService searchTransportService,
                                           SearchPhaseController searchPhaseController, ClusterService clusterService) {
        super("background_search", actionFilters, transportService.getTaskManager());
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.searchPhaseController = searchPhaseController;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, BackgroundSearchRequest request, ActionListener<BackgroundSearchResponse> listener) {
        final long relativeStartNanos = System.nanoTime();
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            request.getSearchRequest().getOrCreateAbsoluteStartMillis(), relativeStartNanos, System::nanoTime);

        ClusterState clusterState = clusterService.state();
        SearchTask searchTask = (SearchTask) task;

        //TODO what are the guarantees here? we get the iterators, filters, routings at submit time, yet the nodes from a more recent
        // cluster state, will that cause problems?

        //doing only local nodes for now, no CCS
        BiFunction<String, String, Transport.Connection> connectionLookup = (clusterAlias, nodeId) ->
            transportService.getConnection(clusterState.nodes().get(nodeId));

        ActionListener<SearchResponse> searchListener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                request.setIntermediateResults(searchResponse);
                //TODO we are risking stack overflow here, maybe each step should be a separate sub-task
                //following batches
                logger.warn("starting another batch");
                run(request, searchTask, timeProvider, connectionLookup, clusterState.version(), listener, this);
            }

            @Override
            public void onFailure(Exception e) {
                //TODO first batch may work, following ones may not. we should rather keep track of failures and move on.
                listener.onFailure(e);
            }
        };

        logger.warn("starting first batch");

        //first batch
        run(request, searchTask, timeProvider, connectionLookup, clusterState.version(), listener, searchListener);
    }

    private void run(BackgroundSearchRequest request, SearchTask task, TransportSearchAction.SearchTimeProvider timeProvider,
                     BiFunction<String, String, Transport.Connection> connectionLookup, long clusterStateVersion,
                     ActionListener<BackgroundSearchResponse> mainListener, ActionListener<SearchResponse> searchListener) {
        GroupShardsIterator<SearchShardIterator> searchShardIterators = request.advanceIterators();
        if (searchShardIterators.size() == 0) {
            //this will trigger storing the result (including the status) in the .task index
            mainListener.onResponse(new BackgroundSearchResponse(request.finalizeResults()));
        } else {
            action(task, request.getSearchRequest(), searchShardIterators, timeProvider, connectionLookup,
                clusterStateVersion, request.getAliasFilter(), request.getConcreteIndexBoosts(), request.getRoutingMap(),
                searchListener, SearchResponse.Clusters.EMPTY).start();
        }
    }

    private AbstractSearchAsyncAction<? extends SearchPhaseResult> action(SearchTask task, SearchRequest searchRequest,
                                                                          GroupShardsIterator<SearchShardIterator> shardIterators,
                                                                          TransportSearchAction.SearchTimeProvider timeProvider,
                                                                          BiFunction<String, String, Transport.Connection> connectionLookup,
                                                                          long clusterStateVersion,
                                                                          Map<String, AliasFilter> aliasFilter,
                                                                          Map<String, Float> concreteIndexBoosts,
                                                                          Map<String, Set<String>> indexRoutings,
                                                                          ActionListener<SearchResponse> listener,
                                                                          SearchResponse.Clusters clusters) {
        Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
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
