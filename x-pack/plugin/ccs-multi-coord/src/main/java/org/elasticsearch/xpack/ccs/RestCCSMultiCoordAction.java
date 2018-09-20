/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xpack.ccs.CCSMultiCoordAction;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.IntConsumer;

public final class RestCCSMultiCoordAction extends BaseRestHandler {

    private static final Set<String> RESPONSE_PARAMS = Collections.singleton(RestSearchAction.TYPED_KEYS_PARAM);

    public RestCCSMultiCoordAction(Settings settings, RestController controller) {
        super(settings);
        //TODO do we need types in here too?
        controller.registerHandler(RestRequest.Method.GET, "/_ccs", this);
        controller.registerHandler(RestRequest.Method.POST, "/_ccs", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_ccs", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_ccs", this);
    }

    @Override
    public String getName() {
        return "ccs_multi_coord_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        /*
         * We have to pull out the call to `source().size(size)` because
         * _update_by_query and _delete_by_query uses this same parsing
         * path but sets a different variable when it sees the `size`
         * url parameter.
         *
         * Note that we can't use `searchRequest.source()::size` because
         * `searchRequest.source()` is null right now. We don't have to
         * guard against it being null in the IntConsumer because it can't
         * be null later. If that is confusing to you then you are in good
         * company.
         */
        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(parser ->
            RestSearchAction.parseSearchRequest(searchRequest, request, parser, setSize));
        return channel -> client.execute(CCSMultiCoordAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
