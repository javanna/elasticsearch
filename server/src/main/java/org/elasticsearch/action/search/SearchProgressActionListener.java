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

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.List;

final class SearchProgressActionListener implements ActionListener<SearchResponse>, SearchProgressListener {

    private final ActionListener<SearchResponse> delegate;
    private final SearchProgressListener searchProgressListener;

    SearchProgressActionListener(ActionListener<SearchResponse> listener, SearchProgressListener searchProgressListener) {
        this.delegate = listener;
        this.searchProgressListener = searchProgressListener;
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        delegate.onResponse(searchResponse);
    }

    @Override
    public void onFailure(Exception e) {
        delegate.onFailure(e);
    }

    @Override
    public void onStart(List<ShardId> shards) {
        searchProgressListener.onStart(shards);
    }

    @Override
    public void onQueryResult(QuerySearchResult result) {
        searchProgressListener.onQueryResult(result);
    }

    @Override
    public void onQueryFailure(int shardId, Exception exc) {
        searchProgressListener.onQueryFailure(shardId, exc);
    }

    @Override
    public void onFetchResult(FetchSearchResult result) {
        searchProgressListener.onFetchResult(result);
    }

    @Override
    public void onFetchFailure(int shardId, Exception exc) {
        searchProgressListener.onFetchFailure(shardId, exc);
    }

    @Override
    public void onPartialReduce(List<SearchShardTarget> shards, TopDocs topDocs, InternalAggregations aggs, int reducePhase) {
        searchProgressListener.onPartialReduce(shards, topDocs, aggs, reducePhase);
    }
}
