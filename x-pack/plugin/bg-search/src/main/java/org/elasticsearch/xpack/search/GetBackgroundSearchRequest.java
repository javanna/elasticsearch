/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetBackgroundSearchRequest extends ActionRequest {

    private final String taskId;

    public GetBackgroundSearchRequest(String taskId) {
        this.taskId = taskId;
    }

    public GetBackgroundSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.taskId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(taskId);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (Strings.isNullOrEmpty(taskId)) {
            return ValidateActions.addValidationError("task_id cannot be null or empty", null);
        }
        return null;
    }

    public String getTaskId() {
        return taskId;
    }
}
