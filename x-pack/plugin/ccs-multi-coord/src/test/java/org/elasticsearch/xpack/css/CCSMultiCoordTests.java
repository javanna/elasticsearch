/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.css;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.SearchRequestConverter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTextAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.PercentageScore;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MovAvgPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

/*
 * This is a duel test between ordinary ccs and ccs alternate execution mode. It does not check response times, but rather correctness,
 * meaning that for the given queries, responses hold the same content for both execution modes.
 *
 * Known limitations:
 *
 * - scroll is not supported: an error is returned, we could rather fallback to ordinary CCS
 *
 * - field collapsing with inner_hits is not supported as only inner hits coming from each cluster can be retrieved as part of
 *   the expand phase (which runs in each remote cluster's coord node as a sub-fetch phase). We can possibly make this work by postponing
 *   the expand round to the last coordination step as it used _msearch so there would be no problems finding the docs.
 *
 * - more_like_this query is not supported when like/unlike items need to be fetched. This cannot properly be intercepted at the moment,
 *   due to fetching that happens on each shard. mlt should be fixed so that this is the same as geo_shape etc. registering async actions.
 *   At the moment documents may not be found, which leads to no search results. This is also a problem today with the current impl of CCS.
 *
 * The following are potentially relevant tests that are currently missing and should be added:
 * - scripted aggs
 * - rescore
 * - suggesters
 * - percolate
 * - parent_child and nested docs
 * - too many buckets and bigarrays allocation
 * - indices boost (though it's broken currently in CCS)
 */
public class CCSMultiCoordTests extends ESTestCase {

    private static RestClient client;

    @BeforeClass
    public static void initClient() {
        client = RestClient.builder(new HttpHost("localhost", 9200)).build();
    }

    @AfterClass
    public static void closeClient() throws IOException {
        IOUtils.close(client);
        client = null;
    }

    public void testMatchAll() throws Exception {
        //verify that the order in which documents are returned when all scored the same (e.g. 1.0) is the same compared
        //to ordinary CCS. That depends on index_uuid and shard_id at the elasticsearch level, segments at the lucene level
        SearchRequest searchRequest = initSearchRequest();
        duelSearch(searchRequest);
    }

    @Ignore
    public void testQueryDifferentIndicesPerCluster() throws Exception {
        SearchRequest searchRequest = new SearchRequest("cluster1:so*", "cluster2:so", "cluster3:foo");
        searchRequest.source().size(0);
        duelSearch(searchRequest);
    }

    @Ignore
    public void testQueryOnlyOneCluster() throws Exception {
        SearchRequest searchRequest = new SearchRequest("cluster3:foo");
        searchRequest.source().size(0);
        duelSearch(searchRequest);
    }

    public void testFullTextQueries() throws Exception  {
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(0);
            sourceBuilder.size(10);
            sourceBuilder.query(QueryBuilders.multiMatchQuery("xml", "title", "body"));
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(0);
            sourceBuilder.size(50);
            sourceBuilder.query(QueryBuilders.multiMatchQuery("java", "title", "body"));
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
    }
    
    public void testFullTextQueriesPagination() throws Exception  {
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(30);
            sourceBuilder.size(25);
            sourceBuilder.query(QueryBuilders.multiMatchQuery("xml", "title", "body"));
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(100);
            sourceBuilder.size(10);
            sourceBuilder.query(QueryBuilders.multiMatchQuery("java", "title", "body"));
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
    }

    public void testFullTextQueriesHighlighting() throws Exception  {
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.highlighter(new HighlightBuilder().field("body"));
            sourceBuilder.query(QueryBuilders.multiMatchQuery("xml", "title", "body"));
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.size(50);
            sourceBuilder.highlighter(new HighlightBuilder().field("title").field("body"));
            sourceBuilder.query(QueryBuilders.multiMatchQuery("java", "title", "body"));
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
    }

    public void testFullTextQueriesFetchSource() throws Exception  {
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.fetchSource(new String[]{"user", "title"}, Strings.EMPTY_ARRAY);
            sourceBuilder.query(QueryBuilders.multiMatchQuery("xml", "title", "body"));
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
    }
    
    public void testFullTextQueriesDocValueFields() throws Exception  {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.docValueField("user.keyword");
        sourceBuilder.query(QueryBuilders.multiMatchQuery("xml", "title", "body"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest);
    }
    
    public void testFullTextQueriesExplain() throws Exception  {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.explain(true);
        sourceBuilder.query(QueryBuilders.multiMatchQuery("xml", "title", "body"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest);
    }

    public void testFullTextQueriesProfile() throws Exception  {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.profile(true);
        sourceBuilder.query(QueryBuilders.multiMatchQuery("java", "title", "body"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest);
    }

    @Ignore
    public void testShardFailures() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:so*");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("creationDate", "err"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest);
    }

    public void testFullTextQueriesSortByField() throws Exception  {
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(30);
            sourceBuilder.size(25);
            sourceBuilder.query(QueryBuilders.multiMatchQuery("xml", "title", "body"));
            sourceBuilder.sort("type.keyword", SortOrder.ASC);
            sourceBuilder.sort("creationDate", SortOrder.DESC);
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
        {
            SearchRequest searchRequest = initSearchRequest();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(100);
            sourceBuilder.size(10);
            sourceBuilder.sort("user.keyword", SortOrder.ASC);
            sourceBuilder.sort("questionId.keyword", SortOrder.ASC);
            sourceBuilder.query(QueryBuilders.multiMatchQuery("java", "title", "body"));
            searchRequest.source(sourceBuilder);
            duelSearch(searchRequest);
        }
    }

    public void testFieldCollapsingSortByScore() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("test", "title", "body"))
            .filter(QueryBuilders.termQuery("type.keyword", "answer"));
        sourceBuilder.query(queryBuilder);
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        duelSearch(searchRequest);
    }

    public void testFieldCollapsingSortByField() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("test", "title", "body"))
            .filter(QueryBuilders.termQuery("type.keyword", "answer"));
        sourceBuilder.query(queryBuilder);
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort(new ScoreSortBuilder());
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        duelSearch(searchRequest);
    }

    @AwaitsFix(bugUrl = "known limitation, we should find a way to fail this type of request")
    public void testFieldCollapsingWithInnerHits() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("test", "title", "body"))
            .filter(QueryBuilders.termQuery("type.keyword", "answer"));
        sourceBuilder.query(queryBuilder);
        sourceBuilder.collapse(new CollapseBuilder("user.keyword").setInnerHits(new InnerHitBuilder("answers").setSize(5)));
        duelSearch(searchRequest);
    }

    public void testTermsAggs() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = termsAggsSource();
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest);
    }

    public void testTermsAggsWithProfile() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = termsAggsSource();
        sourceBuilder.size(0);
        sourceBuilder.profile(true);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest);
    }

    private SearchSourceBuilder termsAggsSource() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder cluster = new TermsAggregationBuilder("cluster", ValueType.STRING);
        cluster.field("_index");
        TermsAggregationBuilder type = new TermsAggregationBuilder("type", ValueType.STRING);
        type.field("type.keyword");
        type.showTermDocCountError(true);
        type.order(BucketOrder.key(true));
        cluster.subAggregation(type);
        sourceBuilder.aggregation(cluster);

        TermsAggregationBuilder tags = new TermsAggregationBuilder("tags", ValueType.STRING);
        tags.field("tags.keyword");
        tags.showTermDocCountError(true);
        tags.size(100);
        sourceBuilder.aggregation(tags);

        TermsAggregationBuilder tags2 = new TermsAggregationBuilder("tags", ValueType.STRING);
        tags2.field("tags.keyword");
        tags.subAggregation(tags2);

        FilterAggregationBuilder answers = new FilterAggregationBuilder("answers", new TermQueryBuilder("type", "answer"));
        TermsAggregationBuilder answerPerQuestion = new TermsAggregationBuilder("answer_per_question", ValueType.STRING);
        answerPerQuestion.showTermDocCountError(true);
        answerPerQuestion.field("questionId.keyword");
        TermsAggregationBuilder answerPerUser = new TermsAggregationBuilder("answer_per_user", ValueType.STRING);
        answerPerUser.field("user.keyword");
        answerPerUser.size(30);
        answerPerUser.showTermDocCountError(true);
        answers.subAggregation(answerPerQuestion);
        answers.subAggregation(answerPerUser);
        sourceBuilder.aggregation(answers);
        return sourceBuilder;
    }
    
    public void testTermsAggsPartitions() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.size(0);
        TermsAggregationBuilder tags = new TermsAggregationBuilder("tags", ValueType.STRING);
        tags.field("tags.keyword");
        tags.size(1000);
        searchSourceBuilder.aggregation(tags);
        for (int i = 0; i < 5; i++) {
            tags.includeExclude(new IncludeExclude(i, 50));
            duelSearch(searchRequest);
        }
    }

    public void testSignificantText() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        SignificantTextAggregationBuilder textAggregationBuilder = new SignificantTextAggregationBuilder("terms", "body");
        textAggregationBuilder.significanceHeuristic(new PercentageScore());
        SamplerAggregationBuilder samplerAggregationBuilder = new SamplerAggregationBuilder("sample").shardSize(1000);
        samplerAggregationBuilder.subAggregation(textAggregationBuilder);
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("tags", ValueType.STRING);
        termsAggregationBuilder.field("tags.keyword");
        termsAggregationBuilder.subAggregation(samplerAggregationBuilder);
        sourceBuilder.aggregation(termsAggregationBuilder);
        duelSearch(searchRequest);
    }
    
    public void testDateHistogram() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        TermsAggregationBuilder tags = new TermsAggregationBuilder("tags", ValueType.STRING);
        tags.field("tags.keyword");
        tags.showTermDocCountError(true);
        DateHistogramAggregationBuilder creation = new DateHistogramAggregationBuilder("creation");
        creation.field("creationDate");
        creation.dateHistogramInterval(DateHistogramInterval.QUARTER);
        creation.subAggregation(tags);
        sourceBuilder.aggregation(creation);
        duelSearch(searchRequest);
    }

    public void testCardinalityAgg() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        CardinalityAggregationBuilder tags = new CardinalityAggregationBuilder("tags", ValueType.STRING);
        tags.field("tags.keyword");
        sourceBuilder.aggregation(tags);
        duelSearch(searchRequest);
    }
    
    public void testTopHits() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        sourceBuilder.size(0);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("type", "question"));
        boolQueryBuilder.must(new MatchQueryBuilder("body", "java spring"));
        sourceBuilder.query(boolQueryBuilder);
        TopHitsAggregationBuilder topHits = new TopHitsAggregationBuilder("top");
        topHits.size(10);
        if (randomBoolean()) {
            topHits.from(10);
        }
        TermsAggregationBuilder tags = new TermsAggregationBuilder("tags", ValueType.STRING);
        tags.field("tags.keyword");
        tags.size(10);
        tags.subAggregation(topHits);
        sourceBuilder.aggregation(tags);
        duelSearch(searchRequest);
    }

    public void testPipelineAggs() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        sourceBuilder.size(0);
        TermsAggregationBuilder type = new TermsAggregationBuilder("type", ValueType.STRING);
        type.field("type.keyword");
        sourceBuilder.aggregation(type);
        DateHistogramAggregationBuilder monthly = new DateHistogramAggregationBuilder("monthly");
        monthly.field("creationDate");
        monthly.dateHistogramInterval(DateHistogramInterval.MONTH);
        type.subAggregation(monthly);
        monthly.subAggregation(new MovAvgPipelineAggregationBuilder("avg", "_count"));
        monthly.subAggregation(new DerivativePipelineAggregationBuilder("derivative", "_count"));
        type.subAggregation(new MaxBucketPipelineAggregationBuilder("biggest_month", "monthly._count"));

        duelSearch(searchRequest);
    }

    public void testMoreLikeThisLikeText() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        MoreLikeThisQueryBuilder mltQueryBuilder = new MoreLikeThisQueryBuilder(new String[]{"body"}, new String[] {"java"}, null);
        sourceBuilder.query(mltQueryBuilder);
        duelSearch(searchRequest);
    }

    public void testTermsLookup() throws Exception {
        Request request = new Request("PUT", "index/type/id");
        request.setEntity(new NStringEntity("{\"tags\":[\"java\", \"jax-ws\"]}", ContentType.APPLICATION_JSON));
        request.addParameter("refresh", "wait_for");
        Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), either(equalTo(200)).or(equalTo(201)));

        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("body", new TermsLookup("index", "type", "id", "tags"));
        sourceBuilder.query(termsQueryBuilder);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest);
    }

    public void testTermsSuggester() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("dstributed exampl androd lbrar");
        suggestBuilder.addSuggestion("body", new TermSuggestionBuilder("body")
            .suggestMode(TermSuggestionBuilder.SuggestMode.POPULAR).minDocFreq(100));
        suggestBuilder.addSuggestion("tags", new TermSuggestionBuilder("tags"));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest);
    }

    public void testPhraseSuggester() throws Exception {
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("loking for an exampl librar");
        suggestBuilder.addSuggestion("body", new PhraseSuggestionBuilder("body").addCandidateGenerator(
            new DirectCandidateGeneratorBuilder("body").suggestMode("always")).highlight("<em>", "</em>"));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest);

    }

    public void testTermsSuggesterGeonames() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:geonames");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("name_bologna", new TermSuggestionBuilder("name")
            .suggestMode(TermSuggestionBuilder.SuggestMode.POPULAR).text("bologa").size(15));
        suggestBuilder.addSuggestion("alternate_bologna", new TermSuggestionBuilder("alternatenames").text("bologa"));
        suggestBuilder.addSuggestion("name_torino", new TermSuggestionBuilder("name")
            .suggestMode(TermSuggestionBuilder.SuggestMode.POPULAR).text("turino").size(40));
        suggestBuilder.addSuggestion("alternate_torino", new TermSuggestionBuilder("alternatenames").text("turino"));
        sourceBuilder.suggest(suggestBuilder);
        suggestBuilder.addSuggestion("name_piacenza", new TermSuggestionBuilder("name")
            .suggestMode(TermSuggestionBuilder.SuggestMode.POPULAR).text("piacenta").size(10));
        suggestBuilder.addSuggestion("alternate_piacenza", new TermSuggestionBuilder("alternatenames").text("piacenta").size(20));

        duelSearch(searchRequest);
    }

    public void testPhraseSuggesterGeonames() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:geonames");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("bolougne firenza, piacenta, turino");
        suggestBuilder.addSuggestion("name", new PhraseSuggestionBuilder("name").addCandidateGenerator(
            new DirectCandidateGeneratorBuilder("name").suggestMode("always")).highlight("<em>", "</em>").size(30));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest);
    }

    public void testCompletionSuggester() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:geonames");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("piacenza", new CompletionSuggestionBuilder("suggest").size(10).text("piace"));
        suggestBuilder.addSuggestion("amsterdam", new CompletionSuggestionBuilder("suggest").size(20).text("amster"));
        suggestBuilder.addSuggestion("bologna", new CompletionSuggestionBuilder("suggest").size(30).text("bolo"));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest);
    }

    public void testCompletionSuggesterWithShardSize() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:geonames");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("piacenza", new CompletionSuggestionBuilder("suggest").size(10).shardSize(50).text("piace"));
        suggestBuilder.addSuggestion("amsterdam", new CompletionSuggestionBuilder("suggest").size(20).shardSize(50).text("amster"));
        suggestBuilder.addSuggestion("bologna", new CompletionSuggestionBuilder("suggest").size(30).shardSize(50).text("bolo"));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest);
    }

    public void testCompletionSuggesterWithSearch() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:geonames");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchQueryBuilder("name", "piacenza"));
        sourceBuilder.size(30);
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("amsterdam", new CompletionSuggestionBuilder("suggest").text("amster").size(10));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest);
    }

    public void testScroll() throws Exception {
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.scroll("1s");
            Request multiCoordRequest = getMultiCoordRequest(searchRequest);
            executeRequest(multiCoordRequest);
        }
        {
            SearchRequest searchRequest = initSearchRequest();
            searchRequest.scroll("1s");
            Request multiCoordRequest = getMultiCoordRequest(searchRequest);
            expectThrows(ResponseException.class, () -> executeRequest(multiCoordRequest));
        }
    }

    private static SearchRequest initSearchRequest() {
        if (randomBoolean()) {
            List<String> indices = Arrays.asList("cluster1:so", "cluster2:so", "cluster3:so");
            Collections.shuffle(indices, random());
            return new SearchRequest(indices.toArray(new String[0]));
        }
        return new SearchRequest("*:so");
    }

    private static void duelSearch(SearchRequest searchRequest) throws Exception {
        Request ccsRequest = SearchRequestConverter.search(searchRequest);
        Map<String, Object> ccs = executeRequest(ccsRequest);
        Request multiCoordRequest = getMultiCoordRequest(searchRequest);
        Map<String, Object> multiCoord = executeRequest(multiCoordRequest);
        assertThat(multiCoord, equalTo(ccs));
    }

    private static Map<String, Object> executeRequest(Request request) throws IOException {
        Response response = client.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, responseBody, true);
        cleanMap(map);
        return map;
    }

    private static Request getMultiCoordRequest(SearchRequest searchRequest) throws IOException {
        Request request = SearchRequestConverter.search(searchRequest);
        Request ccsRequest = new Request(/*request.getMethod()*/"GET", request.getEndpoint().replace("_search", "_ccs"));
        ccsRequest.setEntity(request.getEntity());
        for (Map.Entry<String, String> entry : request.getParameters().entrySet()) {
            ccsRequest.addParameter(entry.getKey(), entry.getValue());
        }
        return ccsRequest;
    }

    @SuppressWarnings("unchecked")
    private static void cleanMap(Map<String, Object> responseMap) {
        assertNotNull(responseMap.put("took", 0L));
        Map<String, Object> profile = (Map<String, Object>)responseMap.get("profile");
        if (profile != null) {
            List<Map<String, Object>> shards = (List <Map<String, Object>>)profile.get("shards");
            for (Map<String, Object> shard : shards) {
                replaceProfileTime(shard);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void replaceProfileTime(Map<String, Object> map) {
        Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            if (entry.getKey().contains("time")) {
                assertThat(entry.getValue(), instanceOf(Number.class));
                iterator.remove();
            }
            if (entry.getKey().equals("breakdown")) {
                Map<String, Long> breakdown = (Map<String, Long>)entry.getValue();
                for (String key : breakdown.keySet()) {
                    assertNotNull(breakdown.put(key, 0L));
                }
            }
            if (entry.getValue() instanceof Map) {
                replaceProfileTime((Map<String, Object>) entry.getValue());
            }
            if (entry.getValue() instanceof List) {
                List<Object> list = (List<Object>)entry.getValue();
                for (Object obj : list) {
                    if (obj instanceof Map) {
                        replaceProfileTime((Map<String, Object>) obj);
                    }
                }
            }
        }
    }
}
