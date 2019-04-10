/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseMerger;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

//TODO does it need to be an indices request for security? maybe we should not even use a TransportAction
//for the background search, it is handy though
public class BackgroundSearchRequest extends ActionRequest {

    private final SearchRequest searchRequest;
    private final Map<String, AliasFilter> aliasFilter;
    private final Map<String, Float> concreteIndexBoosts;
    private final Map<String, Set<String>> routingMap;
    private final State state;

    BackgroundSearchRequest(SearchRequest searchRequest, int batchSize, Map<String, AliasFilter> aliasFilter,
                            Map<String, Float> concreteIndexBoosts, Map<String, Set<String>> routingMap,
                            GroupShardsIterator<SearchShardIterator> searchShardIterators,
                            TransportSearchAction.SearchTimeProvider searchTimeProvider,
                            Function<Boolean, InternalAggregation.ReduceContext> reduceContextFunction) {
        this.searchRequest = searchRequest;
        this.aliasFilter = aliasFilter;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.routingMap = routingMap;
        SearchSourceBuilder source = searchRequest.source();
        final int originalFrom;
        final int originalSize;
        final int trackTotalHitsUpTo;
        if (source == null) {
            originalFrom = SearchService.DEFAULT_FROM;
            originalSize = SearchService.DEFAULT_SIZE;
            trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
        } else {
            originalFrom = source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
            originalSize = source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
            trackTotalHitsUpTo = source.trackTotalHitsUpTo() == null
                ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO : source.trackTotalHitsUpTo();
            //here we modify the original source so we can re-use it by setting it to each outgoing search request
            source.from(0);
            source.size(originalFrom + originalSize);
        }
        this.state = new State(originalFrom, originalSize, trackTotalHitsUpTo, searchShardIterators, reduceContextFunction,
            searchTimeProvider, batchSize);
    }

    SearchRequest getSearchRequest() {
        return searchRequest;
    }

    Map<String, AliasFilter> getAliasFilter() {
        return aliasFilter;
    }

    Map<String, Float> getConcreteIndexBoosts() {
        return concreteIndexBoosts;
    }

    Map<String, Set<String>> getRoutingMap() {
        return routingMap;
    }

    void finalizeResults() {
        this.state.finalizeResults();
    }

    GroupShardsIterator<SearchShardIterator> advanceIterators() {
        return state.advanceIterators();
    }

    void setIntermediateResults(SearchResponse searchResponse) {
        state.setIntermediateResults(searchResponse);
    }

    @Override
    public ActionRequestValidationException validate() {
        //this request is only created internally, no need to validate
        return null;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        // generating description in a lazy way since source can be quite big
        return new BackgroundSearchTask(id, type, action, null, parentTaskId, headers, this.state) {
            @Override
            public String getDescription() {
                return searchRequest.getTaskDescription();
            }
        };
    }

    @Override
    public boolean getShouldStoreResult() {
        return true;
    }

    @Override
    public void readFrom(StreamInput in) {
        throw new UnsupportedOperationException("readFrom is not supported");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("writeTo is not supported");
    }

    static final class State {
        private final int originalFrom;
        private final int originalSize;
        private final int trackTotalHitsUpTo;
        private final GroupShardsIterator<SearchShardIterator> searchShardIterators;
        private final Function<Boolean, InternalAggregation.ReduceContext> reduceContextFunction;
        private final TransportSearchAction.SearchTimeProvider searchTimeProvider;
        private final int batchSize;

        private final AtomicInteger fromShard = new AtomicInteger(0);
        private final AtomicReference<SearchResponse> intermediateResults = new AtomicReference<>();

        private State(int originalFrom, int originalSize, int trackTotalHitsUpTo,
                      GroupShardsIterator<SearchShardIterator> searchShardIterators,
                      Function<Boolean, InternalAggregation.ReduceContext> reduceContextFunction,
                      TransportSearchAction.SearchTimeProvider searchTimeProvider, int batchSize) {
            this.originalFrom = originalFrom;
            this.originalSize = originalSize;
            this.trackTotalHitsUpTo = trackTotalHitsUpTo;
            this.searchShardIterators = searchShardIterators;
            this.reduceContextFunction = reduceContextFunction;
            this.searchTimeProvider = searchTimeProvider;
            this.batchSize = batchSize;
        }

        GroupShardsIterator<SearchShardIterator> advanceIterators() {
            //TODO maybe we should be smarter around the selection of the shards, maybe group them per index?
            int currentFromShard = fromShard.getAndAdd(batchSize);
            int counter = 0;
            List<SearchShardIterator> iteratorList = new ArrayList<>();
            for (SearchShardIterator iterator : searchShardIterators) {
                if (counter >= currentFromShard) {
                    if (counter < currentFromShard + batchSize) {
                        iteratorList.add(iterator);
                    } else {
                        break;
                    }
                }
                counter++;
            }
            return new GroupShardsIterator<>(iteratorList);
        }

        int getRunningFromShard() {
            return fromShard.get() - batchSize;
        }

        int getBatchSize() {
            return batchSize;
        }

        int getTotalShards() {
            return searchShardIterators.size();
        }

        void setIntermediateResults(SearchResponse searchResponse) {
            //TODO the intermediate results need to be persisted somewhere
            intermediateResults.accumulateAndGet(searchResponse,
                (previous, next) -> previous == null ? next : SearchResponseMerger.getMergedResponse(SearchResponse.Clusters.EMPTY,
                    0, originalFrom + originalSize, trackTotalHitsUpTo, searchTimeProvider,
                    reduceContextFunction.apply(false), previous, next));
        }

        //TODO improve concurrency aspect here, synchronized is not good.
        synchronized void finalizeResults() {
            intermediateResults.getAndUpdate(this::performFinalReduce);
            assert fromShard.get() + batchSize >= searchShardIterators.size();
            fromShard.set(searchShardIterators.size());
        }

        SearchResponse retrieveIntermediateResults() {
            //TODO here we do a final reduction at each call, maybe we should cache this given that clients are going to poll get task
            //maybe even skip returning the response unless it's changed.
            SearchResponse searchResponse = intermediateResults.get();
            //here we would need to introduce an intermediate state so that e.g. date histogram does not create empty buckets at this stage
            return searchResponse == null ? null : performFinalReduce(searchResponse);
        }

        private SearchResponse performFinalReduce(SearchResponse searchResponse) {
            //TODO test this when there are no shards in the first place and finalize is called before any response is set.
            return SearchResponseMerger.getMergedResponse(SearchResponse.Clusters.EMPTY,
                originalFrom, originalSize, trackTotalHitsUpTo, searchTimeProvider, reduceContextFunction.apply(true), searchResponse);
        }
    }
}
