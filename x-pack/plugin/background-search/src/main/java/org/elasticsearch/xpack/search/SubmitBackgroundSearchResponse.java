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

    SubmitBackgroundSearchResponse(TaskId taskId) {
        this.taskId = taskId;
    }

    SubmitBackgroundSearchResponse(StreamInput in) throws IOException {
        this(TaskId.readFromStream(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskId.writeTo(out);
    }

    public TaskId getTaskId() {
        return taskId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        //TODOwe should sign the id or something along those line to secure retrieving search response only through a specific endpoint
        builder.field("task_id", taskId.toString());
        builder.endObject();
        return builder;
    }
}
