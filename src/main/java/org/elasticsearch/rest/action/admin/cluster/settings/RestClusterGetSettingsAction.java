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

package org.elasticsearch.rest.action.admin.cluster.settings;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

/**
 */
public class RestClusterGetSettingsAction extends BaseActionRequestRestHandler<ClusterStateRequest> {

    @Inject
    public RestClusterGetSettingsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/settings", this);
    }

    @Override
    protected ClusterStateRequest newRequest(RestRequest request) {
        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .listenerThreaded(false)
                .routingTable(false)
                .nodes(false);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        return clusterStateRequest;
    }

    @Override
    public void doHandleRequest(final RestRequest restRequest, final RestChannel channel, ClusterStateRequest request) {
        client.admin().cluster().state(request, new RestBuilderListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();

                builder.startObject("persistent");
                response.getState().metaData().persistentSettings().toXContent(builder, restRequest);
                builder.endObject();

                builder.startObject("transient");
                response.getState().metaData().transientSettings().toXContent(builder, restRequest);
                builder.endObject();

                builder.endObject();

                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
