/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.CanMatchPreFilterSearchPhase;
import org.elasticsearch.action.search.SearchPhase;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

public class TransportSubmitBackgroundSearchAction
    extends HandledTransportAction<SubmitBackgroundSearchRequest, SubmitBackgroundSearchResponse> {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final SearchService searchService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NodeClient client;
    private final TransportBackgroundSearchAction transportBackgroundSearchAction;

    @Inject
    public TransportSubmitBackgroundSearchAction(ThreadPool threadPool, TransportService transportService, SearchService searchService,
                                                 SearchTransportService searchTransportService,
                                                 ClusterService clusterService, ActionFilters actionFilters, Client client,
                                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                                 TransportBackgroundSearchAction transportBackgroundSearchAction) {
        super(SubmitBackgroundSearchAction.NAME, transportService, actionFilters, SubmitBackgroundSearchRequest::new);
        this.threadPool = threadPool;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportService = transportService;
        this.transportBackgroundSearchAction = transportBackgroundSearchAction;
        this.client = (NodeClient)client;
    }

    //TODO there is quite some copy pasted code from TransportSearchAction here, we need to make to effort to share as much as possible

    @Override
    protected void doExecute(Task task, SubmitBackgroundSearchRequest submitBackgroundSearchRequest,
                             ActionListener<SubmitBackgroundSearchResponse> listener) {

        SearchRequest originalSearchRequest = submitBackgroundSearchRequest.getSearchRequest();
        SearchRequest searchRequest = new SearchRequest(originalSearchRequest, originalSearchRequest.indices(), null,
            originalSearchRequest.getOrCreateAbsoluteStartMillis(), false);

        //TODO does took matter in each search response at all?
        final long relativeStartNanos = System.nanoTime();
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            searchRequest.getOrCreateAbsoluteStartMillis(), relativeStartNanos, System::nanoTime);

        ActionListener<SearchSourceBuilder> rewriteListener = ActionListener.wrap(source -> {
            if (source != searchRequest.source()) {
                // only set it if it changed - we don't allow null values to be set but it might be already null. this way we catch
                // situations when source is rewritten to null due to a bug
                searchRequest.source(source);
            }

            //TODO we are only supporting local search at the moment, would CCS fit into this? Probably only when not minimizing roundtrips?
            final ClusterState clusterState = clusterService.state();
            Index[] indices = indexNameExpressionResolver.concreteIndices(clusterState, searchRequest.indicesOptions(),
                timeProvider.getAbsoluteStartMillis(), searchRequest.indices());
            Map<String, AliasFilter> aliasFilter = buildPerIndexAliasFilter(searchRequest, clusterState, indices, Collections.emptyMap());
            String[] concreteIndices = new String[indices.length];
            for (int i = 0; i < indices.length; i++) {
                concreteIndices[i] = indices[i].getName();
            }
            Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(),
                searchRequest.indices());
            routingMap = routingMap == null ? Collections.emptyMap() : Collections.unmodifiableMap(routingMap);

            clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

            Map<String, Float> concreteIndexBoosts = resolveIndexBoosts(searchRequest, clusterState);

            Map<String, Long> nodeSearchCounts = searchTransportService.getPendingSearchRequests();
            GroupShardsIterator<ShardIterator> localShardsIterator = clusterService.operationRouting().searchShards(clusterState,
                concreteIndices, routingMap, searchRequest.preference(), searchService.getResponseCollectorService(), nodeSearchCounts,
                new ShardComparator(clusterState, searchService));
            List<SearchShardIterator> shards = new ArrayList<>();
            OriginalIndices localIndices = new OriginalIndices(searchRequest);
            for (ShardIterator shardIterator : localShardsIterator) {
                shards.add(new SearchShardIterator(null, shardIterator.shardId(), shardIterator.getShardRoutings(), localIndices));
            }
            GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(shards);

            //TODO is this needed?
            //failIfOverShardCountLimit(clusterService, shardIterators.size());

            if (searchRequest.allowPartialSearchResults() == null) {
                // No user preference defined in search request - apply cluster service default
                searchRequest.allowPartialSearchResults(searchService.defaultAllowPartialSearchResults());
            }

            //TODO not sure if these two optimizations are needed here. sounds like edge cases that people would not use this API for.
            // optimize search type for cases where there is only one shard group to search on
            if (shardIterators.size() == 1) {
                // if we only have one group, then we always want Q_T_F, no need for DFS, and no need to do THEN since we hit one shard
                searchRequest.searchType(QUERY_THEN_FETCH);
            }
            if (searchRequest.isSuggestOnly()) {
                // disable request cache if we have only suggest
                searchRequest.requestCache(false);
                if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
                    // convert to Q_T_F if we have only suggest
                    searchRequest.searchType(QUERY_THEN_FETCH);
                }
            }

            //doing only local nodes for now, no CCS
            BiFunction<String, String, Transport.Connection> connectionLookup = (clusterAlias, nodeId) ->
                transportService.getConnection(clusterState.nodes().get(nodeId));

            //we can't do this for DFS it needs to fan out to all shards all the time.
            boolean preFilterSearchShards = searchRequest.searchType() == QUERY_THEN_FETCH && SearchService.canRewriteToMatchNone(source);

            SearchTask searchTask = (SearchTask) task;

            Map<String, Set<String>> routings = routingMap;
            long clusterStateVersion = clusterState.version();
            ActionListener<SearchResponse> canMatchListener = ActionListener.map(listener, response -> {
                throw new IllegalStateException("unexpected call to onResponse while running can_match phase");
            });

            //TODO can match can be done for each batch instead of once
            Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
            if (preFilterSearchShards) {
                new CanMatchPreFilterSearchPhase(logger, searchTransportService, connectionLookup, aliasFilter, concreteIndexBoosts,
                    routings, executor, searchRequest, canMatchListener, shardIterators, timeProvider, clusterStateVersion, searchTask,
                    iterators -> new SearchPhase("submit_background_search") {
                        @Override
                        public void run() {
                            //TODO here shall we count the potential shard failures when calling can_match and return them?
                            runTask(searchRequest, submitBackgroundSearchRequest.getBatchSize(), timeProvider, aliasFilter,
                                concreteIndexBoosts, routings, iterators,
                                shardIterators.size(), searchService::createReduceContext, listener);
                        }
                    }, SearchResponse.Clusters.EMPTY);
            } else {
                runTask(searchRequest, submitBackgroundSearchRequest.getBatchSize(), timeProvider, aliasFilter, concreteIndexBoosts,
                    routings, shardIterators, shardIterators.size(), searchService::createReduceContext, listener);
            }
        }, listener::onFailure);

        if (searchRequest.source() == null) {
            rewriteListener.onResponse(searchRequest.source());
        } else {
            Rewriteable.rewriteAndFetch(searchRequest.source(), searchService.getRewriteContext(timeProvider::getAbsoluteStartMillis),
                rewriteListener);
        }
    }

    private void runTask(SearchRequest searchRequest, int batchSize, TransportSearchAction.SearchTimeProvider timeProvider,
                         Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts, Map<String, Set<String>> routingMap,
                         GroupShardsIterator<SearchShardIterator> searchShardIterators, int totalShards,
                         Function<Boolean, InternalAggregation.ReduceContext> reduceContextFunction,
                         ActionListener<SubmitBackgroundSearchResponse> listener) {

        BackgroundSearchRequest request = new BackgroundSearchRequest(searchRequest, batchSize, aliasFilter, concreteIndexBoosts,
            routingMap, searchShardIterators, timeProvider, reduceContextFunction);
        //TODO here we execute on the same thread, we fork only once we call the search async action,
        //that should be ok, we could also potentially get the first batch of results.
        Task task = transportBackgroundSearchAction.execute(request, LoggingTaskListener.instance());
        //TODO maybe it would be better to just register a task manually, but TransportAction handles storing results too.
        listener.onResponse(new SubmitBackgroundSearchResponse(new TaskId(client.getLocalNodeId(), task.getId())));
    }

    private Map<String, AliasFilter> buildPerIndexAliasFilter(SearchRequest request, ClusterState clusterState,
                                                              Index[] concreteIndices, Map<String, AliasFilter> remoteAliasMap) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), indicesAndAliases);
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        aliasFilterMap.putAll(remoteAliasMap);
        return aliasFilterMap;
    }

    private Map<String, Float> resolveIndexBoosts(SearchRequest searchRequest, ClusterState clusterState) {
        if (searchRequest.source() == null) {
            return Collections.emptyMap();
        }

        SearchSourceBuilder source = searchRequest.source();
        if (source.indexBoosts() == null) {
            return Collections.emptyMap();
        }

        Map<String, Float> concreteIndexBoosts = new HashMap<>();
        for (SearchSourceBuilder.IndexBoost ib : source.indexBoosts()) {
            Index[] concreteIndices =
                indexNameExpressionResolver.concreteIndices(clusterState, searchRequest.indicesOptions(), ib.getIndex());

            for (Index concreteIndex : concreteIndices) {
                concreteIndexBoosts.putIfAbsent(concreteIndex.getUUID(), ib.getBoost());
            }
        }
        return Collections.unmodifiableMap(concreteIndexBoosts);
    }

    private static final class ShardComparator implements Comparator<ShardIterator> {
        private final ClusterState clusterState;
        private final SearchService searchService;

        ShardComparator(ClusterState clusterState, SearchService searchService) {
            this.clusterState = clusterState;
            this.searchService = searchService;
        }

        @Override
        public int compare(ShardIterator o1, ShardIterator o2) {
            ShardId shardId1 = o1.shardId();
            IndexSettings indexSettings1 = searchService.getIndicesService().indexServiceSafe(shardId1.getIndex()).getIndexSettings();
            ShardId shardId2 = o2.shardId();
            IndexSettings indexSettings2 = searchService.getIndicesService().indexServiceSafe(shardId2.getIndex()).getIndexSettings();
            if (indexSettings1.isSearchThrottled() == indexSettings2.isSearchThrottled()) {
                IndexMetaData indexMetaData1 = clusterState.metaData().index(shardId1.getIndex());
                IndexMetaData indexMetaData2 = clusterState.metaData().index(shardId2.getIndex());
                //TODO the split and shrink API override the creation date, which messes up ordering based on it.
                return Long.compare(indexMetaData1.getCreationDate(), indexMetaData2.getCreationDate());
            }
            if (indexSettings1.isSearchThrottled()) {
                return -1;
            } else {
                return 1;
            }
        }
    };
}
