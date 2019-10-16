/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.CanMatchPreFilterSearchPhase;
import org.elasticsearch.action.search.SearchIndicesResolver;
import org.elasticsearch.action.search.SearchPhase;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
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
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
    private final SearchIndicesResolver searchIndicesResolver;
    private final NodeClient client;
    private final RemoteClusterService remoteClusterService;

    @Inject
    public TransportSubmitBackgroundSearchAction(ThreadPool threadPool, TransportService transportService, SearchService searchService,
                                                 SearchTransportService searchTransportService,
                                                 ClusterService clusterService, ActionFilters actionFilters,
                                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                                 NodeClient nodeClient) {
        super(SubmitBackgroundSearchAction.NAME, transportService, actionFilters, SubmitBackgroundSearchRequest::new);
        this.threadPool = threadPool;
        this.searchTransportService = searchTransportService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.searchIndicesResolver = new SearchIndicesResolver(searchService, indexNameExpressionResolver);
        this.transportService = transportService;
        this.client = nodeClient;
    }

    //TODO there is quite some copy pasted code from TransportSearchAction here, we need to make to effort to share as much as possible

    @Override
    protected void doExecute(Task task, SubmitBackgroundSearchRequest submitBackgroundSearchRequest,
                             ActionListener<SubmitBackgroundSearchResponse> listener) {

        SearchRequest searchRequest = submitBackgroundSearchRequest.getSearchRequest();

        final long relativeStartNanos = System.nanoTime();
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            searchRequest.getOrCreateAbsoluteStartMillis(), relativeStartNanos, System::nanoTime);

        ActionListener<SearchSourceBuilder> rewriteListener = ActionListener.wrap(source -> {
            if (source != searchRequest.source()) {
                // only set it if it changed - we don't allow null values to be set but it might be already null. this way we catch
                // situations when source is rewritten to null due to a bug
                searchRequest.source(source);
            }
            final ClusterState clusterState = clusterService.state();
            final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(searchRequest.indicesOptions(),
                searchRequest.indices());
            OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            //TODO do we want to support CCS?
            if (remoteClusterIndices.isEmpty() == false) {
                throw new IllegalArgumentException("remote indices are not supported by background search");
            }

            clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

            SearchRequest.ResolvedIndex[] resolvedIndices = searchIndicesResolver.resolveIndices(localIndices, searchRequest,
                timeProvider, Collections.emptyMap(), clusterState);

            Arrays.sort(resolvedIndices, (index1, index2) -> {
                IndexMetaData indexMetaData1 = clusterState.metaData().index(index1.getIndexName());
                IndexSettings indexSettings1 = searchService.getIndicesService().indexServiceSafe(index1.getIndex()).getIndexSettings();
                IndexMetaData indexMetaData2 = clusterState.metaData().index(index2.getIndexName());
                IndexSettings indexSettings2 = searchService.getIndicesService().indexServiceSafe(index2.getIndex()).getIndexSettings();
                if (indexSettings1.isSearchThrottled() == indexSettings2.isSearchThrottled()) {
                    //TODO how should isSearchThrottled influence the grouping?
                }
                //TODO once we support CCS remote indices should also be groupd separately?
                return Long.compare(indexMetaData2.getCreationDate(), indexMetaData1.getCreationDate());
            });



            //TODO how do we set pre_filter_search_shards?

            //TODO run one task per group
            //TODO here we execute on the same thread, we fork only once we call the search async action,
            //that should be ok, we could also potentially get the first batch of results.
            //Task task = client.executeLocally(TransportBackgroundSearchAction.TYPE, request, LoggingTaskListener.instance());
            //TODO maybe it would be better to just register a task manually, but TransportAction handles storing results too.
            //listener.onResponse(new SubmitBackgroundSearchResponse(new TaskId(client.getLocalNodeId(), task.getId())));

        }, listener::onFailure);

        if (searchRequest.source() == null) {
            rewriteListener.onResponse(searchRequest.source());
        } else {
            Rewriteable.rewriteAndFetch(searchRequest.source(), searchService.getRewriteContext(timeProvider::getAbsoluteStartMillis),
                rewriteListener);
        }
    }
}
