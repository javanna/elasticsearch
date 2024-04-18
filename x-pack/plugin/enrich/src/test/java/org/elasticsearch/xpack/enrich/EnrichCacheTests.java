/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class EnrichCacheTests extends ESTestCase {

    public void testCaching() {
        // Emulate cluster metadata:
        // (two enrich indices with corresponding alias entries)
        var metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy1") + "-1")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy1")).build())
            )
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy2") + "-1")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy2")).build())
            )
            .build();

        // Emulated search requests that an enrich processor could generate:
        // (two unique searches for two enrich policies)
        var searchRequest1 = new SearchRequest(EnrichPolicy.getBaseName("policy1")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "1"))
        );
        var searchRequest2 = new SearchRequest(EnrichPolicy.getBaseName("policy1")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "2"))
        );
        var searchRequest3 = new SearchRequest(EnrichPolicy.getBaseName("policy2")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "1"))
        );
        var searchRequest4 = new SearchRequest(EnrichPolicy.getBaseName("policy2")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "2"))
        );
        // Emulated search response (content doesn't matter, since it isn't used, it just a cache entry)
        List<Map<?, ?>> searchResponse = List.of(Map.of("test", "entry"));

        EnrichCache enrichCache = new EnrichCache(3);
        enrichCache.setMetadata(metadata);
        enrichCache.put(searchRequest1, searchResponse);
        enrichCache.put(searchRequest2, searchResponse);
        enrichCache.put(searchRequest3, searchResponse);
        var cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(0L));
        assertThat(cacheStats.getMisses(), equalTo(0L));
        assertThat(cacheStats.getEvictions(), equalTo(0L));

        assertThat(enrichCache.get(searchRequest1), notNullValue());
        assertThat(enrichCache.get(searchRequest2), notNullValue());
        assertThat(enrichCache.get(searchRequest3), notNullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(3L));
        assertThat(cacheStats.getMisses(), equalTo(1L));
        assertThat(cacheStats.getEvictions(), equalTo(0L));

        enrichCache.put(searchRequest4, searchResponse);
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(3L));
        assertThat(cacheStats.getMisses(), equalTo(1L));
        assertThat(cacheStats.getEvictions(), equalTo(1L));

        // Simulate enrich policy execution, which should make current cache entries unused.
        metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy1") + "-2")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy1")).build())
            )
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy2") + "-2")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy2")).build())
            )
            .build();
        enrichCache.setMetadata(metadata);

        // Because enrich index has changed, cache can't serve cached entries
        assertThat(enrichCache.get(searchRequest1), nullValue());
        assertThat(enrichCache.get(searchRequest2), nullValue());
        assertThat(enrichCache.get(searchRequest3), nullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());

        // Add new entries using new enrich index name as key
        enrichCache.put(searchRequest1, searchResponse);
        enrichCache.put(searchRequest2, searchResponse);
        enrichCache.put(searchRequest3, searchResponse);

        // Entries can now be served:
        assertThat(enrichCache.get(searchRequest1), notNullValue());
        assertThat(enrichCache.get(searchRequest2), notNullValue());
        assertThat(enrichCache.get(searchRequest3), notNullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(6L));
        assertThat(cacheStats.getMisses(), equalTo(6L));
        assertThat(cacheStats.getEvictions(), equalTo(4L));
    }

    public void testPutIfAbsent() throws InterruptedException {
        // Emulate cluster metadata:
        // (two enrich indices with corresponding alias entries)
        var metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy1") + "-1")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy1")).build())
            )
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy2") + "-1")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy2")).build())
            )
            .build();

        // Emulated search requests that an enrich processor could generate:
        // (two unique searches for two enrich policies)
        var searchRequest1 = new SearchRequest(EnrichPolicy.getBaseName("policy1")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "1"))
        );
        final List<Map<String, ?>> searchResponseMap = List.of(
            Map.of("key1", "value1", "key2", "value2"),
            Map.of("key3", "value3", "key4", "value4")
        );
        EnrichCache enrichCache = new EnrichCache(3);
        enrichCache.setMetadata(metadata);

        {
            CountDownLatch queriedDatabaseLatch = new CountDownLatch(1);
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent(searchRequest1, (searchRequest, searchResponseActionListener) -> {
                SearchResponse searchResponse = convertToSearchResponse(searchResponseMap);
                searchResponseActionListener.onResponse(searchResponse);
                searchResponse.decRef();
                queriedDatabaseLatch.countDown();
            }, new ActionListener<>() {
                @Override
                public void onResponse(List<Map<?, ?>> response) {
                    assertThat(response, equalTo(searchResponseMap));
                    notifiedOfResultLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });
            assertThat(queriedDatabaseLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
        }

        {
            CountDownLatch notifiedOfResultLatch = new CountDownLatch(1);
            enrichCache.computeIfAbsent(searchRequest1, (searchRequest, searchResponseActionListener) -> {
                fail("Expected no call to the database because item should have been in the cache");
            }, new ActionListener<>() {
                @Override
                public void onResponse(List<Map<?, ?>> maps) {
                    notifiedOfResultLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });
            assertThat(notifiedOfResultLatch.await(5, TimeUnit.SECONDS), equalTo(true));
        }
    }

    private SearchResponse convertToSearchResponse(List<Map<String, ?>> searchResponseList) {
        SearchHit[] hitArray = searchResponseList.stream().map(map -> {
            try {
                return SearchHit.unpooled(0, "id").sourceRef(convertMapToJson(map));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toArray(SearchHit[]::new);
        SearchHits hits = SearchHits.unpooled(hitArray, null, 0);
        return new SearchResponse(
            hits,
            null,
            null,
            false,
            false,
            null,
            1,
            null,
            5,
            4,
            0,
            randomLong(),
            null,
            SearchResponse.Clusters.EMPTY
        );
    }

    private BytesReference convertMapToJson(Map<String, ?> simpleMap) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(simpleMap)) {
            return BytesReference.bytes(builder);
        }
    }

    public void testDeepCopy() {
        Map<String, Object> original = new HashMap<>();
        {
            original.put("foo", "bar");
            original.put("int", 123);
            original.put("double", 123.0D);
            Map<String, Object> innerObject = new HashMap<>();
            innerObject.put("buzz", "hello world");
            innerObject.put("foo_null", null);
            innerObject.put("1", "bar");
            innerObject.put("long", 123L);
            List<String> innerInnerList = new ArrayList<>();
            innerInnerList.add("item1");
            List<Object> innerList = new ArrayList<>();
            innerList.add(innerInnerList);
            innerObject.put("list", innerList);
            original.put("fizz", innerObject);
            List<Map<String, Object>> list = new ArrayList<>();
            Map<String, Object> value = new HashMap<>();
            value.put("field", "value");
            list.add(value);
            list.add(null);
            original.put("list", list);
            List<String> list2 = new ArrayList<>();
            list2.add("foo");
            list2.add("bar");
            list2.add("baz");
            original.put("list2", list2);
        }

        Map<?, ?> result = EnrichCache.deepCopy(original, false);
        assertThat(result, equalTo(original));
        assertThat(result, not(sameInstance(original)));

        result = EnrichCache.deepCopy(original, true);
        assertThat(result, equalTo(original));
        assertThat(result, not(sameInstance(original)));
        Map<?, ?> innerMap = (Map<?, ?>) result.get("fizz");
        expectThrows(UnsupportedOperationException.class, () -> innerMap.remove("x"));
        List<?> innerList = (List<?>) result.get("list");
        expectThrows(UnsupportedOperationException.class, () -> innerList.remove(0));

        original.put("embedded_object", new byte[] { 1, 2, 3 });
        result = EnrichCache.deepCopy(original, false);
        assertArrayEquals(new byte[] { 1, 2, 3 }, (byte[]) result.get("embedded_object"));
    }

    public void testEnrichIndexNotExist() {
        // Emulate cluster metadata:
        var metadata = Metadata.builder().build();

        // Emulated search request on a non-exist enrich index that an enrich processor could generate
        var searchRequest = new SearchRequest(EnrichPolicy.getBaseName("policy-enrich-index-not-generated")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("test", "query"))
        );
        // Emulated search response (content doesn't matter, since it isn't used, it just a cache entry)
        List<Map<?, ?>> searchResponse = List.of(Map.of("test", "entry"));

        EnrichCache enrichCache = new EnrichCache(1);
        enrichCache.setMetadata(metadata);

        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> enrichCache.put(searchRequest, searchResponse));
        assertThat(e.getMessage(), containsString("no generated enrich index [.enrich-policy-enrich-index-not-generated]"));
    }

}
