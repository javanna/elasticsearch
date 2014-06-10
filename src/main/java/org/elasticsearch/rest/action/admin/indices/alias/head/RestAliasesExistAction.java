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

package org.elasticsearch.rest.action.admin.indices.alias.head;

import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;

import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class RestAliasesExistAction extends BaseActionRequestRestHandler<GetAliasesRequest> {

    @Inject
    public RestAliasesExistAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HEAD, "/_alias/{name}", this);
        controller.registerHandler(HEAD, "/{index}/_alias/{name}", this);
        controller.registerHandler(HEAD, "/{index}/_alias", this);
    }

    @Override
    protected GetAliasesRequest newRequest(RestRequest request) {
        String[] aliases = request.paramAsStringArray("name", Strings.EMPTY_ARRAY);
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(aliases);
        getAliasesRequest.indices(indices);
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));
        return getAliasesRequest;
    }

    @Override
    public void doHandleRequest(final RestRequest restRequest, final RestChannel channel, GetAliasesRequest request) {
        client.admin().indices().aliasesExist(request, new RestResponseListener<AliasesExistResponse>(channel) {
            @Override
            public RestResponse buildResponse(AliasesExistResponse response) {
                if (response.isExists()) {
                    return new BytesRestResponse(OK);
                } else {
                    return new BytesRestResponse(NOT_FOUND);
                }
            }
        });
    }
}
