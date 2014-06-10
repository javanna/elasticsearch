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

package org.elasticsearch.rest.action.bench;

import org.elasticsearch.action.bench.AbortBenchmarkRequest;
import org.elasticsearch.action.bench.AbortBenchmarkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseActionRequestRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for benchmark abort action.
 */
public class RestBenchAbortAction extends BaseActionRequestRestHandler<AbortBenchmarkRequest> {

    @Inject
    public RestBenchAbortAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_bench/abort/{name}", this);
    }

    @Override
    protected AbortBenchmarkRequest newRequest(RestRequest request) {
        final String[] benchmarkNames = Strings.splitStringByCommaToArray(request.param("name"));
        return new AbortBenchmarkRequest(benchmarkNames);
    }

    @Override
    protected void doHandleRequest(RestRequest restRequest, RestChannel restChannel, AbortBenchmarkRequest request) {
        client.abortBench(request, new AcknowledgedRestListener<AbortBenchmarkResponse>(restChannel));
    }
}
