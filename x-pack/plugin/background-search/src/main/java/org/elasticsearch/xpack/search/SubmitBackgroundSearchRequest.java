/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public final class SubmitBackgroundSearchRequest extends ActionRequest implements IndicesRequest.Replaceable, TaskAwareRequest {

    //TODO enforce that a scroll is not set, as it's not supported, etc.
    private final SearchRequest searchRequest;

    SubmitBackgroundSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    SubmitBackgroundSearchRequest(StreamInput in) throws IOException {
        this.searchRequest = new SearchRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.searchRequest.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return searchRequest.validate();
    }

    @Override
    public String[] indices() {
        return searchRequest.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return searchRequest.indicesOptions();
    }

    @Override
    public IndicesRequest indices(String... indices) {
        return searchRequest.indices(indices);
    }

    SearchRequest getSearchRequest() {
        return searchRequest;
    }
}
