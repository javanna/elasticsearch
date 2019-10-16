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
package org.elasticsearch.action.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class SearchIndicesResolver {
    private final SearchService searchService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public SearchIndicesResolver(SearchService searchService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.searchService = searchService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public SearchRequest.ResolvedIndex[] resolveIndices(OriginalIndices localIndices,
                                                         SearchRequest searchRequest,
                                                         TransportSearchAction.SearchTimeProvider timeProvider,
                                                         Map<String, AliasFilter> remoteAliasMap,
                                                         ClusterState clusterState) {
        // TODO: I think startTime() should become part of ActionRequest and that should be used both for index name
        // date math expressions and $now in scripts. This way all apis will deal with now in the same way instead
        // of just for the _search api
        final Index[] indices = resolveLocalIndices(localIndices, searchRequest.indicesOptions(), clusterState, timeProvider);
        Map<String, AliasFilter> aliasFilter = buildPerIndexAliasFilter(searchRequest, clusterState, indices, remoteAliasMap);
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(),
            searchRequest.indices());
        routingMap = routingMap == null ? Collections.emptyMap() : Collections.unmodifiableMap(routingMap);
        Map<String, Float> concreteIndexBoosts = resolveIndexBoosts(searchRequest, clusterState);

        SearchRequest.ResolvedIndex[] resolvedIndices = new SearchRequest.ResolvedIndex[indices.length];
        for (int i = 0; i < indices.length; i++) {
            Index index = indices[i];
            AliasFilter indexAliasFilter = aliasFilter.get(index.getUUID());
            Float indexBoost = concreteIndexBoosts.get(index.getUUID());
            Set<String> routingValues = routingMap.get(index.getName());
            resolvedIndices[i] = new SearchRequest.ResolvedIndex(index, indexAliasFilter, indexBoost, routingValues);
        }
        return resolvedIndices;
    }

    private Index[] resolveLocalIndices(OriginalIndices localIndices,
                                        IndicesOptions indicesOptions,
                                        ClusterState clusterState,
                                        TransportSearchAction.SearchTimeProvider timeProvider) {
        if (localIndices == null) {
            return Index.EMPTY_ARRAY; //don't search on any local index (happens when only remote indices were specified)
        }
        return indexNameExpressionResolver.concreteIndices(clusterState, indicesOptions,
            timeProvider.getAbsoluteStartMillis(), localIndices.indices());
    }

    private Map<String, AliasFilter> buildPerIndexAliasFilter(SearchRequest request, ClusterState clusterState,
                                                              Index[] concreteIndices, Map<String, AliasFilter> remoteAliasMap) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), indicesAndAliases);
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        aliasFilterMap.putAll(remoteAliasMap);
        return aliasFilterMap;
    }

    private Map<String, Float> resolveIndexBoosts(SearchRequest searchRequest, ClusterState clusterState) {
        if (searchRequest.source() == null) {
            return Collections.emptyMap();
        }

        SearchSourceBuilder source = searchRequest.source();
        if (source.indexBoosts() == null) {
            return Collections.emptyMap();
        }

        Map<String, Float> concreteIndexBoosts = new HashMap<>();
        for (SearchSourceBuilder.IndexBoost ib : source.indexBoosts()) {
            Index[] concreteIndices =
                indexNameExpressionResolver.concreteIndices(clusterState, searchRequest.indicesOptions(), ib.getIndex());

            for (Index concreteIndex : concreteIndices) {
                concreteIndexBoosts.putIfAbsent(concreteIndex.getUUID(), ib.getBoost());
            }
        }
        return Collections.unmodifiableMap(concreteIndexBoosts);
    }
}
