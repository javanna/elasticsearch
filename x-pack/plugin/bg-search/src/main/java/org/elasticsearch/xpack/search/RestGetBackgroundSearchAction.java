/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.TaskId;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetBackgroundSearchAction extends BaseRestHandler  {

    public RestGetBackgroundSearchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_bgsearch/{task_id}", this);
    }

    @Override
    public String getName() {
        return "background_search_get_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setTaskId(new TaskId(request.param("task_id")));
        return channel -> client.execute(GetBackgroundSearchAction.INSTANCE, getTaskRequest, new RestToXContentListener<>(channel));
    }
}
