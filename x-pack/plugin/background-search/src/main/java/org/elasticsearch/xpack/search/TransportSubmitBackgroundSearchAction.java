/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchProgressListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TransportSubmitBackgroundSearchAction
    extends HandledTransportAction<SubmitBackgroundSearchRequest, SubmitBackgroundSearchResponse> {

    private final NodeClient client;

    @Inject
    public TransportSubmitBackgroundSearchAction(TransportService transportService,
                                                 ActionFilters actionFilters,
                                                 NodeClient nodeClient) {
        super(SubmitBackgroundSearchAction.NAME, transportService, actionFilters, SubmitBackgroundSearchRequest::new);
        this.client = nodeClient;
    }

    @Override
    protected void doExecute(Task task, SubmitBackgroundSearchRequest submitBackgroundSearchRequest,
                             ActionListener<SubmitBackgroundSearchResponse> listener) {

        SearchRequest searchRequest = submitBackgroundSearchRequest.getSearchRequest();
        searchRequest.setParentTask(client.getLocalNodeId(), task.getId());

        //TODO sorting of shards needs to be implemented in search action directly, no grouping needed as long as
        //we can follow the progress of the corresponding search that we start

        //TODO this is quite a hack which required to make SearchRequest non final. The problem is that we want to use the
        //search task directly rather than having an additional task that is parent of the search one, yet we want some enhanced
        // status associated to the task that we execute.
        BackgroundSearchStatus backgroundSearchStatus = new BackgroundSearchStatus();
        SearchRequest backgroundSearchRequest = new SearchRequest(searchRequest, backgroundSearchStatus) {
            @Override
            public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                return new BackgroundSearchTask(task, backgroundSearchStatus);
            }

            @Override
            public boolean getShouldStoreResult() {
                return true;
            }
        };

        //TODO how often do we want to perform non final reduction? This determines how often intermediate results are available.
        backgroundSearchRequest.setBatchedReduceSize(3);

        Task searchTask = client.executeLocally(SearchAction.INSTANCE, backgroundSearchRequest, ActionListener.wrap(r -> {}, e -> {}));
        TaskId backgroundSearchTaskId = new TaskId(client.getLocalNodeId(), searchTask.getId());
        //TODO add the wait for timeout mechanism, also return results that may be already available
        listener.onResponse(new SubmitBackgroundSearchResponse(backgroundSearchTaskId));
    }

    static class BackgroundSearchStatus implements SearchProgressListener, Task.Status {

        static final String NAME = "background_search_status";

        private final SetOnce<Integer> totalShards = new SetOnce<>();
        private final AtomicInteger completedOperations = new AtomicInteger(0);
        private final AtomicReference<InternalAggregations> partialAggs = new AtomicReference<>();

        BackgroundSearchStatus() {

        }

        BackgroundSearchStatus(StreamInput in) throws IOException {
            totalShards.set(in.readVInt());
            completedOperations.set(in.readVInt());
            InternalAggregations aggregations = in.readOptionalWriteable(InternalAggregations::new);
            if (aggregations != null) {
                partialAggs.set(aggregations);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(totalShards.get());
            out.writeVInt(completedOperations.get());
            out.writeOptionalWriteable(partialAggs.get());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void onStart(List<ShardId> shards) {
            totalShards.set(shards.size());
        }

        @Override
        public void onQueryResult(QuerySearchResult result) {
            completedOperations.incrementAndGet();
        }

        @Override
        public void onQueryFailure(int shardId, Exception exc) {
            //TODO how do we find out if this is the last attempt for the shard, hence we need to increment the counter?
            completedOperations.incrementAndGet();
        }

        @Override
        public void onFetchResult(FetchSearchResult result) {
            completedOperations.incrementAndGet();
        }

        @Override
        public void onFetchFailure(int shardId, Exception exc) {
            completedOperations.incrementAndGet();
        }

        @Override
        public void onPartialReduce(List<SearchShardTarget> shards, TopDocs topDocs, InternalAggregations aggs, int reducePhase) {
            partialAggs.set(aggs);
        }

        int getTotalShards() {
            Integer totalShards = this.totalShards.get();
            return totalShards == null ? -1 : totalShards;
        }

        int getCompletedOperations() {
            return completedOperations.get();
        }

        InternalAggregations getPartialAggs() {
            return partialAggs.get();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }
    }
}
