package org.elasticsearch.action.search;

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.List;

public interface SearchActionListener extends ActionListener<SearchResponse> {
    /**
     *
     *
     * @param shards
     */
    void onStart(List<ShardId> shards);

    /**
     *
     * @param result
     */
    void onQueryResult(QuerySearchResult result);

    /**
     *
     * @param shardId
     * @param exc
     */
    void onQueryFailure(int shardId, Exception exc);

    /**
     *
     * @param result
     */
    void onFetchResult(FetchSearchResult result);

    /**
     *
     * @param shardId
     * @param exc
     */
    void onFetchFailure(int shardId, Exception exc);

    /**
     *
     */
    void onPartialReduce(List<SearchShardTarget> shards, TopDocs topDocs, InternalAggregations aggs, int reducePhase);
}
