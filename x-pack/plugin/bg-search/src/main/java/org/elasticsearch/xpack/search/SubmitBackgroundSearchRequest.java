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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

//TODO maybe this class is not even needed, SearchRequest can be used directly?
public final class SubmitBackgroundSearchRequest extends ActionRequest implements IndicesRequest.Replaceable, TaskAwareRequest {

    private final SearchRequest searchRequest;

    public SubmitBackgroundSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    SubmitBackgroundSearchRequest(StreamInput in) throws IOException {
        this.searchRequest = new SearchRequest(in);
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

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public int getBatchSize() {
        //TODO find a reasonable value/heuristic for this value. Should we make it a setting? or should the submit request take its value?
        return 2;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return searchRequest.createTask(id, type, action, parentTaskId, headers);
    }
}
