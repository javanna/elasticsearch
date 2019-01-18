/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccs;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WIP: transport action for ccs alternate execution mode that runs multiple coordination steps.
 * It may be convenient to integrate this in TransportSearchAction and enable it through a flag, so that
 * msearch would support it as well. In case we do so we would need to add support for the same flag to the count API as well.
 * Also, in case we make the expand search phase work, we'd need to pass this flag to the inner msearch request.
 */
public final class TransportCCSMultiCoordAction extends TransportAction<SearchRequest, SearchResponse> {

    private final ClusterService clusterService;
    private final RemoteClusterService remoteClusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;
    private final TransportSearchAction transportSearchAction;
    private final SearchService searchService;

    @Inject
    public TransportCCSMultiCoordAction(String actionName, ActionFilters actionFilters, ClusterService clusterService,
                                        TransportService transportService, IndexNameExpressionResolver indexNameExpressionResolver,
                                        ThreadPool threadPool, TransportSearchAction transportSearchAction, SearchService searchService) {
        super(actionName, actionFilters, transportService.getTaskManager());
        this.clusterService = clusterService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = threadPool;
        this.transportSearchAction = transportSearchAction;
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        final long absoluteStartMillis = System.currentTimeMillis();
        final long relativeStartNanos = System.nanoTime();
        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(absoluteStartMillis, relativeStartNanos, System::nanoTime);

        ClusterState clusterState = clusterService.state();
        final Map<String, OriginalIndices> groupedIndices = remoteClusterService.groupIndices(searchRequest.indicesOptions(),
            searchRequest.indices(), idx -> indexNameExpressionResolver.hasIndexOrAlias(idx, clusterState));

        assert groupedIndices.isEmpty() == false;

        if (groupedIndices.size() == 1 && groupedIndices.containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
            //this optimization is not really needed, it just prevents us from throwing the errors below unless needed.
            transportSearchAction.execute(task, searchRequest, listener);
        } else {
            if (searchRequest.scroll() != null) {
                listener.onFailure(new IllegalArgumentException("Scroll is not supported by CCS alternate execution mode"));
                return;
            }

            final SearchSourceBuilder sourceBuilder = searchRequest.source();
            int originalFrom = sourceBuilder.from() == -1 ? 0 : sourceBuilder.from();
            int originalSize = sourceBuilder.size() == -1 ? 10 : sourceBuilder.size();
            sourceBuilder.from(0);
            sourceBuilder.size(originalFrom + originalSize);

            QueryRewriteContext rewriteContext = searchService.getRewriteContext(timeProvider::getAbsoluteStartMillis);

            Rewriteable.rewriteAndFetch(searchRequest.source(), rewriteContext, ActionListener.wrap(source -> {
                if (source != searchRequest.source()) {
                    // only set it if it changed - we don't allow null values to be set but it might be already null be we want to catch
                    // situations when it possible due to a bug changes to null
                    searchRequest.source(source);
                }
                final List<SearchResponse> responses = new CopyOnWriteArrayList<>();
                final AtomicReference<Exception> exceptions = new AtomicReference<>();
                final CountDown countDown = new CountDown(groupedIndices.size());
                for (Map.Entry<String, OriginalIndices> entry : groupedIndices.entrySet()) {
                    String cluster = entry.getKey();
                    String[] indices = entry.getValue().indices();
                    SearchRequest request = createRequestForMultiCoord(searchRequest, indices, cluster);

                    ActionListener<SearchResponse> multiCoordListener = new ActionListener<SearchResponse>() {
                        @Override
                        public void onResponse(SearchResponse searchResponse) {
                            responses.add(searchResponse);
                            if (countDown.countDown()) {
                                Exception exception = exceptions.get();
                                if (exception == null) {
                                    listener.onResponse(merge(originalFrom, originalSize, responses,
                                        searchService.createReduceContext(true)));
                                } else {
                                    //TODO here we need to support skip_unavailable
                                    listener.onFailure(exceptions.get());
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            Exception exception;
                            if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(cluster)) {
                                exception = e;
                            } else {
                                exception = new RemoteTransportException("error while communicating with remote cluster [" +
                                    cluster + "]", e);
                            }
                            if (exceptions.compareAndSet(null, exception) == false) {
                                exception = exceptions.accumulateAndGet(exception, (previous, current) -> {
                                    current.addSuppressed(previous);
                                    return current;
                                });
                            }
                            //it can happen that onResponse throws and calls onFailure, then countDown
                            //is not enough or we may never notify the listener of the failure
                            if (countDown.isCountedDown() || countDown.countDown()) {
                                listener.onFailure(exception);
                            }
                        }
                    };
                    if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(cluster)) {
                        transportSearchAction.execute(task, request, multiCoordListener);
                    } else {
                        //TODO is it ok that we always create a new client object?
                        Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, cluster);
                        remoteClusterClient.search(request, multiCoordListener);
                    }
                }
            }, listener::onFailure));
        }
    }

    public static SearchRequest createRequestForMultiCoord(SearchRequest originalSearchRequest, String[] indices, String clusterAlias) {
        SearchRequest searchRequest = new SearchRequest(originalSearchRequest);
        searchRequest.indices(indices);
        searchRequest.setPerformFinalReduce(false);
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
            searchRequest.setIndexPrefix(clusterAlias);
        }
        return searchRequest;
    }

    @SuppressWarnings("rawtypes")
    public static SearchResponse merge(int from, int size, List<SearchResponse> searchResponses,
                                       InternalAggregation.ReduceContext reduceContext) {

        int totalShards = 0;
        int skippedShards = 0;
        int successfulShards = 0;
        long tookInMillis = 0;
        float maxScore = Float.NEGATIVE_INFINITY;
        List<ShardSearchFailure> failures = new ArrayList<>();
        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        List<InternalAggregations> aggs = new ArrayList<>();
        Map<String, List<Suggestion>> groupedSuggestions = new HashMap<>();
        Map<ShardId, List<CompletionSuggestion.Entry.Option>> completionSuggestionOptions = new TreeMap<>();
        List<TopDocs> topDocsList = new ArrayList<>(searchResponses.size());
        //save space by removing duplicates, yet sort based on natural ordering at the same time
        Map<ShardId, ShardIdWithShardIndex> shardIds = new TreeMap<>();
        for (SearchResponse searchResponse : searchResponses) {
            totalShards += searchResponse.getTotalShards();
            skippedShards += searchResponse.getSkippedShards();
            successfulShards += searchResponse.getSuccessfulShards();
            //TODO not too sure about this, it should probably include the merge time as well?
            tookInMillis = Math.max(tookInMillis, searchResponse.getTook().millis());
            SearchHits searchHits = searchResponse.getHits();
            if (Float.isNaN(searchHits.getMaxScore()) == false) {
                maxScore = Math.max(maxScore, searchHits.getMaxScore());
            }
            Collections.addAll(failures, searchResponse.getShardFailures());

            profileResults.putAll(searchResponse.getProfileResults());

            if (searchResponse.getAggregations() != null) {
                InternalAggregations internalAggs = (InternalAggregations) searchResponse.getAggregations();
                aggs.add(internalAggs);
            }

            SearchHit[] hits = searchHits.getHits();
            ScoreDoc[] scoreDocs = new ScoreDoc[hits.length];

            Suggest suggest = searchResponse.getSuggest();
            if (suggest != null) {
                List<CompletionSuggestion> completionSuggestions = suggest.filter(CompletionSuggestion.class);
                for (CompletionSuggestion completionSuggestion : completionSuggestions) {
                    for (CompletionSuggestion.Entry.Option option : completionSuggestion.getOptions()) {
                        ShardId shardId = option.getHit().getShard().getShardId();
                        List<CompletionSuggestion.Entry.Option> options =
                            completionSuggestionOptions.computeIfAbsent(shardId, id -> new ArrayList<>());
                        options.add(option);
                    }
                }
                for (Suggestion<? extends Suggestion.Entry<? extends Suggestion.Entry.Option>> suggestion : suggest) {
                    List<Suggestion> suggestionList = groupedSuggestions.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                    suggestionList.add(suggestion);
                }
            }

            //TODO replace with the TotalHits instance obtained once available
            TotalHits totalHits = new TotalHits(searchHits.totalHits, TotalHits.Relation.EQUAL_TO);
            final TopDocs topDocs;
            if (searchHits.getSortFields() != null) {
                if (searchHits.getCollapseField() != null) {
                    assert searchHits.getCollapseValues() != null;
                    topDocs = new CollapseTopFieldDocs(searchHits.getCollapseField(), totalHits, scoreDocs,
                        searchHits.getSortFields(), searchHits.getCollapseValues());

                } else {
                    topDocs = new TopFieldDocs(totalHits, scoreDocs, searchHits.getSortFields());
                }
            } else {
                topDocs = new TopDocs(totalHits, scoreDocs);
            }
            topDocsList.add(topDocs);

            for (int i = 0; i < hits.length; i++) {
                SearchHit hit = hits[i];
                ShardId shardId = hit.getShard().getShardId();
                ShardIdWithShardIndex shardIdWithShardIndex = shardIds.computeIfAbsent(shardId, ShardIdWithShardIndex::new);
                final SortField[] sortFields = searchHits.getSortFields();
                if (sortFields == null) {
                    scoreDocs[i] = new ScoreDocAndSearchHit(hit.docId(), hit.getScore(), hit, shardIdWithShardIndex);
                } else {
                    final Object[] sortValues;
                    if (sortFields.length == 1 && sortFields[0].getType() == SortField.Type.SCORE) {
                        sortValues = new Object[]{hit.getScore()};
                    } else {
                        //in case we are really sorted by field we need to parse back the formatted values (String to BytesRef)
                        sortValues = hit.getSearchSortValues().parseSortValues();
                    }
                    scoreDocs[i] = new FieldDocAndSearchHit(hit.docId(), hit.getScore(), sortValues, hit, shardIdWithShardIndex);
                }
            }
        }

        //now that we've gone through all the hits and we collected all the shards they come from, we can assign shardIndex to each shard
        //with this loop we remove the need to look-up once again the shardId in the map for each hit in the following hits loop
        int shardIndex = 0;
        for (ShardIdWithShardIndex shardIdWithShardIndex : shardIds.values()) {
            shardIdWithShardIndex.shardIndex = shardIndex++;
        }
        for (TopDocs topDocs : topDocsList) {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                ((SearchHitSyncShardIndex)scoreDoc).syncShardIndex();
            }
        }

        TopDocs topDocs = SearchPhaseController.mergeTopDocs(topDocsList, size, from);
        //TODO when can topDocs be null here?
        SearchHit[] searchHitArray = new SearchHit[topDocs.scoreDocs.length];
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            SearchHitSyncShardIndex scoreDoc = (SearchHitSyncShardIndex)topDocs.scoreDocs[i];
            searchHitArray[i] = scoreDoc.hit();
        }

        maxScore = Float.isInfinite(maxScore) ? Float.NaN : maxScore;
        SearchHits searchHits = new SearchHits(searchHitArray, topDocs.totalHits.value, maxScore);

        InternalAggregations reducedAggs = InternalAggregations.reduce(aggs, reduceContext);

        int shardCounter = 0;
        for (List<CompletionSuggestion.Entry.Option> options : completionSuggestionOptions.values()) {
            for (CompletionSuggestion.Entry.Option option : options) {
                option.setShardIndex(shardCounter);
            }
            shardCounter++;
        }
        Suggest suggest = groupedSuggestions.isEmpty() ? null : new Suggest(Suggest.reduce(groupedSuggestions));

        //TODO numReducePhases should probably reflect the multiple coordination steps?
        //TODO terminatedEarly and timedOut: how do we merge those?
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, reducedAggs,
            suggest, profileResults.isEmpty() ? null : new SearchProfileShardResults(profileResults), false, null, 1);
        //TODO compute proper Clusters section and support skip_unavailable through listener
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(searchResponses.size(), searchResponses.size(), 0);
        //resort the failures so they are ordered the same as ordinary ccs
        failures.sort((o1, o2) -> {
            ShardId shardId1 = extractShardId(o1);
            ShardId shardId2 = extractShardId(o2);
            if (shardId1 == null && shardId2 == null) {
                return 0;
            }
            if (shardId1 == null) {
                return -1;
            }
            if (shardId2 == null) {
                return 1;
            }
            return shardId1.compareTo(shardId2);
        });
        return new SearchResponse(internalSearchResponse, null, totalShards, successfulShards, skippedShards, tookInMillis,
            failures.toArray(ShardSearchFailure.EMPTY_ARRAY), clusters);
    }

    private static ShardId extractShardId(ShardSearchFailure failure) {
        SearchShardTarget shard = failure.shard();
        if (shard != null) {
            return shard.getShardId();
        }
        Throwable cause = failure.getCause();
        if (cause instanceof ElasticsearchException) {
            ElasticsearchException e = (ElasticsearchException) cause;
            return e.getShardId();
        }
        //TODO we could potentially also take the int ShardSearchFailure#shardId together with the Index from the cause.
        // The String ShardSearchFailure#index() is not that useful as it does not have the uuid
        return null;
    }

    private static final class ShardIdWithShardIndex extends ShardId {
        int shardIndex = -1;

        ShardIdWithShardIndex(ShardId shardId) {
            super(shardId.getIndex(), shardId.getId());
        }
    }

    private static final class ScoreDocAndSearchHit extends ScoreDoc implements SearchHitSyncShardIndex {
        private final SearchHit searchHit;
        private final ShardIdWithShardIndex shardId;

        ScoreDocAndSearchHit(int doc, float score, SearchHit searchHit, ShardIdWithShardIndex shardIdWithShardIndex) {
            super(doc, score);
            this.searchHit = searchHit;
            this.shardId = shardIdWithShardIndex;
        }

        @Override
        public void syncShardIndex() {
            this.shardIndex = shardId.shardIndex;
        }

        @Override
        public SearchHit hit() {
            return searchHit;
        }
    }

    private static final class FieldDocAndSearchHit extends FieldDoc implements SearchHitSyncShardIndex {
        private final SearchHit searchHit;
        private final ShardIdWithShardIndex shardId;

        FieldDocAndSearchHit(int doc, float score, Object[] fields, SearchHit searchHit, ShardIdWithShardIndex shardIdWithShardIndex) {
            super(doc, score, fields);
            this.searchHit = searchHit;
            this.shardId = shardIdWithShardIndex;
        }

        @Override
        public void syncShardIndex() {
            this.shardIndex = shardId.shardIndex;
        }

        @Override
        public SearchHit hit() {
            return searchHit;
        }
    }

    interface SearchHitSyncShardIndex {
        SearchHit hit();
        void syncShardIndex();

    }
}
