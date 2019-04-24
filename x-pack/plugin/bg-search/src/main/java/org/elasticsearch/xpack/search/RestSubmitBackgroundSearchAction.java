/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.function.IntConsumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public final class RestSubmitBackgroundSearchAction extends BaseRestHandler {

    public RestSubmitBackgroundSearchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_bgsearch/submit", this);
        controller.registerHandler(POST, "/{index}/_bgsearch/submit", this);
    }

    @Override
    public String getName() {
        return "background_search_submit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(parser ->
            RestSearchAction.parseSearchRequest(searchRequest, request, parser, setSize));
        SubmitBackgroundSearchRequest submitBackgroundSearchRequest = new SubmitBackgroundSearchRequest(searchRequest);
        if (request.hasParam("batch_size")) {
            submitBackgroundSearchRequest.setBatchSize(request.paramAsInt("batch_size", -1));
        }
        return channel -> client.execute(SubmitBackgroundSearchAction.INSTANCE, submitBackgroundSearchRequest,
            new RestToXContentListener<>(channel));
    }
}
