/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public class SubmitBackgroundSearchResponse extends ActionResponse implements ToXContentObject {

    private final TaskId taskId;
    private final int totalShards;
    private final int skippedShards;

    SubmitBackgroundSearchResponse(TaskId taskId, int totalShards, int skippedShards) {
        this.taskId = taskId;
        this.totalShards = totalShards;
        this.skippedShards = skippedShards;
    }

    SubmitBackgroundSearchResponse(StreamInput in) throws IOException {
        this(TaskId.readFromStream(in), in.readVInt(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskId.writeTo(out);
        out.writeVInt(totalShards);
        out.writeVInt(skippedShards);
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public int getTotalShards() {
        return totalShards;
    }

    public int getSkippedShards() {
        return skippedShards;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        //maybe return also some info about the shards to be processed?
        builder.field("task_id", taskId);
        builder.startObject("_shards");
        builder.field("total", totalShards);
        builder.field("skipped", skippedShards);
        builder.field("to_be_processed", totalShards - skippedShards);
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
