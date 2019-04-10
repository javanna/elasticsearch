/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.transport.TransportService;

public class TransportGetBackgroundSearchAction extends HandledTransportAction<GetTaskRequest, GetBackgroundSearchResponse> {

    private final Client client;

    @Inject
    public TransportGetBackgroundSearchAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetBackgroundSearchAction.NAME, transportService, actionFilters, GetTaskRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetTaskRequest request, ActionListener<GetBackgroundSearchResponse> listener) {

        client.admin().cluster().getTask(request, new ActionListener<GetTaskResponse>() {
            @Override
            public void onResponse(GetTaskResponse getTaskResponse) {

            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private static void process(GetTaskRequest request, TaskResult taskResult) {
        Task.Status status = taskResult.getTask().getStatus();
        if (status instanceof BackgroundSearchTask.BackgroundSearchStatus) {
            BackgroundSearchTask.BackgroundSearchStatus searchStatus = (BackgroundSearchTask.BackgroundSearchStatus) status;


        } else {
            throw new IllegalArgumentException("The task identified by the provided id [" + request.getTaskId() +
                "] is not a BackgroundSearchTask");
        }
    }
}
