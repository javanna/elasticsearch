/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices.refresh;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 *
 */
public class RestRefreshAction extends BaseActionRequestRestHandler<RefreshRequest> {

    @Inject
    public RestRefreshAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_refresh", this);
        controller.registerHandler(POST, "/{index}/_refresh", this);

        controller.registerHandler(GET, "/_refresh", this);
        controller.registerHandler(GET, "/{index}/_refresh", this);
    }

    @Override
    protected RefreshRequest newRequest(RestRequest request) {
        RefreshRequest refreshRequest = new RefreshRequest(Strings.splitStringByCommaToArray(request.param("index")));
        refreshRequest.listenerThreaded(false);
        refreshRequest.force(request.paramAsBoolean("force", refreshRequest.force()));
        refreshRequest.indicesOptions(IndicesOptions.fromRequest(request, refreshRequest.indicesOptions()));
        return refreshRequest;
    }

    @Override
    public void doHandleRequest(final RestRequest restRequest, final RestChannel channel, RefreshRequest request) {
        client.admin().indices().refresh(request, new RestBuilderListener<RefreshResponse>(channel) {
            @Override
            public RestResponse buildResponse(RefreshResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                buildBroadcastShardsHeader(builder, response);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
