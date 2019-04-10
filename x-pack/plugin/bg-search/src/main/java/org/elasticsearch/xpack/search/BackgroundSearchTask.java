/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class BackgroundSearchTask extends SearchTask {

    private final BackgroundSearchRequest.State backgroundSearchState;

    BackgroundSearchTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers,
                         BackgroundSearchRequest.State backgroundSearchState) {
        super(id, type, action, description, parentTaskId, headers);
        this.backgroundSearchState = backgroundSearchState;
    }

    @Override
    public Status getStatus() {
        int processed = backgroundSearchState.getTotalShards() - backgroundSearchState.getRunningFromShard();
        return new BackgroundSearchStatus(processed, backgroundSearchState.getBatchSize(),
            backgroundSearchState.getTotalShards(), backgroundSearchState.retrieveIntermediateResults());
    }

    static class BackgroundSearchStatus implements Task.Status {

        static final String NAME = "background_search_status";

        private final int processed;
        private final int batchSize;
        private final int totalShards;
        private final SearchResponse results;

        private BackgroundSearchStatus(int processed, int batchSize, int totalShards, SearchResponse results) {
            this.processed = processed;
            this.batchSize = batchSize;
            this.totalShards = totalShards;
            this.results = results;
        }

        BackgroundSearchStatus(StreamInput in) throws IOException {
            this.processed = in.readVInt();
            this.batchSize = in.readVInt();
            this.totalShards = in.readVInt();
            this.results = new SearchResponse();
            this.results.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(processed);
            out.writeVInt(batchSize);
            out.writeVInt(totalShards);
            this.results.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            //we always return info as part of the status, even once the task is completed. That guarantees consistency no matter
            //if the status is returned on-the-fly or retrieved from .task index. The completed flag returned by task API may be off though
            //as we have final response and mark all shards completed a little before the results are stored.
            builder.startObject();
            builder.startObject("_shards");
            builder.field("processed", processed);
            builder.field("batch_size", batchSize);
            builder.field("total", totalShards);
            builder.endObject();
            builder.field("results", results);
            builder.endObject();
            return builder;
        }
    }
}

