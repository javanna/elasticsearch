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
package org.elasticsearch.rest.action.admin.indices;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetIndicesAction extends BaseRestHandler {

    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;

    public RestGetIndicesAction(Settings settings, RestController controller, IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter) {
        super(settings);
        this.indexScopedSettings = indexScopedSettings;
        controller.registerHandler(GET, "/{index}", this);
        controller.registerHandler(GET, "/{index}/{type}", this);
        this.settingsFilter = settingsFilter;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] featureParams = request.paramAsStringArray("type", null);
        // Work out if the indices is a list of features
        if (featureParams == null && indices.length > 0 && indices[0] != null && indices[0].startsWith("_") && !"_all".equals(indices[0])) {
            featureParams = indices;
            indices = new String[] {"_all"};
        }
        final GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indices);
        if (featureParams != null) {
            Feature[] features = Feature.convertToFeatures(featureParams);
            getIndexRequest.features(features);
        }
        getIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, getIndexRequest.indicesOptions()));
        getIndexRequest.local(request.paramAsBoolean("local", getIndexRequest.local()));
        final boolean defaults = request.paramAsBoolean("include_defaults", false);
        return channel -> client.admin().indices().getIndex(getIndexRequest, new RestBuilderListener<GetIndexResponse>(channel) {

            @Override
            public RestResponse buildResponse(GetIndexResponse response, XContentBuilder builder) throws Exception {
                Feature[] features = getIndexRequest.features();
                String[] indices = response.indices();

                builder.startObject();
                for (String index : indices) {
                    builder.startObject(index);
                    for (Feature feature : features) {
                        switch (feature) {
                        case ALIASES:
                            writeAliases(response.aliases().get(index), builder, request);
                            break;
                        case MAPPINGS:
                            writeMappings(response.mappings().get(index), builder, request);
                            break;
                        case SETTINGS:
                            writeSettings(response.settings().get(index), builder, request, defaults);
                            break;
                        default:
                            throw new IllegalStateException("feature [" + feature + "] is not valid");
                        }
                    }
                    builder.endObject();

                }
                builder.endObject();

                return new BytesRestResponse(OK, builder);
            }

            private void writeAliases(List<AliasMetaData> aliases, XContentBuilder builder, Params params) throws IOException {
                builder.startObject(Fields.ALIASES);
                if (aliases != null) {
                    for (AliasMetaData alias : aliases) {
                        AliasMetaData.Builder.toXContent(alias, builder, params);
                    }
                }
                builder.endObject();
            }

            private void writeMappings(ImmutableOpenMap<String, MappingMetaData> mappings, XContentBuilder builder, Params params)
                    throws IOException {
                builder.startObject(Fields.MAPPINGS);
                if (mappings != null) {
                    for (ObjectObjectCursor<String, MappingMetaData> typeEntry : mappings) {
                        builder.field(typeEntry.key);
                        builder.map(typeEntry.value.sourceAsMap());
                    }
                }
                builder.endObject();
            }

            private void writeSettings(Settings settings, XContentBuilder builder, Params params, boolean defaults) throws IOException {
                builder.startObject(Fields.SETTINGS);
                if (builder.humanReadable()) {
                    settings = IndexMetaData.addHumanReadableSettings(settings);
                }
                settings.toXContent(builder, params);
                builder.endObject();
                if (defaults) {
                    builder.startObject("defaults");
                    settingsFilter
                        .filter(indexScopedSettings.diff(settings, RestGetIndicesAction.this.settings))
                        .toXContent(builder, request);
                    builder.endObject();
                }
            }

        });
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    static class Fields {
        static final String ALIASES = "aliases";
        static final String MAPPINGS = "mappings";
        static final String SETTINGS = "settings";
    }

}
