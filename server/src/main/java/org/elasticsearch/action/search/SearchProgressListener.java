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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.List;

/**
 * Listener used to monitor progress of a running search request. Gets notified whenever
 * - the search starts
 * - new shard results are available for the query phase
 * - a shard fails to execute the query phase
 * - new shard results are available for the fetch phase
 * - a shard fails to execute the fetch phase
 * - an incremental reduction has been completed
 */
public interface SearchProgressListener {
    void onStart(List<ShardId> shards);

    void onQueryResult(QuerySearchResult result);

    void onQueryFailure(int shardId, Exception exc);

    void onFetchResult(FetchSearchResult result);

    void onFetchFailure(int shardId, Exception exc);

    void onPartialReduce(List<SearchShardTarget> shards, TopDocs topDocs, InternalAggregations aggs, int reducePhase);

    SearchProgressListener NOOP = new SearchProgressListener() {
        @Override
        public void onStart(List<ShardId> shards) {

        }

        @Override
        public void onQueryResult(QuerySearchResult result) {

        }

        @Override
        public void onQueryFailure(int shardId, Exception exc) {

        }

        @Override
        public void onFetchResult(FetchSearchResult result) {

        }

        @Override
        public void onFetchFailure(int shardId, Exception exc) {

        }

        @Override
        public void onPartialReduce(List<SearchShardTarget> shards, TopDocs topDocs, InternalAggregations aggs, int reducePhase) {

        }
    };
}
