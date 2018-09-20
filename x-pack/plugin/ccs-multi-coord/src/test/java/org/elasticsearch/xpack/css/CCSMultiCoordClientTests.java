package org.elasticsearch.xpack.css;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.ccs.TransportCCSMultiCoordAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CCSMultiCoordClientTests extends ESTestCase {

    private static Map<String, TransportClient> transportClientMap = new HashMap<>();
    private static BigArrays bigArrays = new BigArrays(new PageCacheRecycler(Settings.EMPTY), null);
    private static ScriptService scriptService = new ScriptService(Settings.EMPTY, Collections.emptyMap(), Collections.emptyMap());

    @BeforeClass
    public static void initClients() throws UnknownHostException {
        PreBuiltTransportClient cluster1 =
            new PreBuiltTransportClient(Settings.builder().put("cluster.name", "cluster1").build(), Collections.emptyList());
        cluster1.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9301));
        transportClientMap.put("cluster1", cluster1);
        PreBuiltTransportClient cluster2 =
            new PreBuiltTransportClient(Settings.builder().put("cluster.name", "cluster2").build(), Collections.emptyList());
        cluster2.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9302));
        transportClientMap.put("cluster2", cluster2);
        PreBuiltTransportClient cluster3 =
            new PreBuiltTransportClient(Settings.builder().put("cluster.name", "cluster3").build(), Collections.emptyList());
        cluster3.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9303));
        transportClientMap.put("cluster3", cluster3);
    }

    @AfterClass
    public static void closeClients() throws IOException {
        IOUtils.close(transportClientMap.values());
        transportClientMap.clear();
        transportClientMap = null;
    }

    public void testFieldCollapsing() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:so");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.multiMatchQuery("test", "title", "body"))
            .filter(QueryBuilders.termQuery("type.keyword", "answer"));
        sourceBuilder.query(queryBuilder);
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        SearchResponse searchResponse = search(searchRequest);
        System.out.println(Strings.toString(searchResponse, true, true));
    }

    public void testMatchAll() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:so");
        SearchResponse searchResponse = search(searchRequest);
        System.out.println(Strings.toString(searchResponse, true, true));
    }

    public void testSortByField() throws Exception {
        SearchRequest searchRequest = new SearchRequest("*:so");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(30);
        sourceBuilder.size(25);
        sourceBuilder.query(QueryBuilders.multiMatchQuery("xml", "title", "body"));
        sourceBuilder.sort("type.keyword", SortOrder.ASC);
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = search(searchRequest);
        System.out.println(Strings.toString(searchResponse, true, true));
    }

    private static SearchResponse search(SearchRequest searchRequest) throws InterruptedException {
        final SearchSourceBuilder sourceBuilder = searchRequest.source();
        int originalFrom = sourceBuilder.from() == -1 ? 0 : sourceBuilder.from();
        int originalSize = sourceBuilder.size() == -1 ? 10 : sourceBuilder.size();
        sourceBuilder.from(0);
        sourceBuilder.size(originalFrom + originalSize);

        CountDownLatch latch = new CountDownLatch(transportClientMap.size());
        List<SearchResponse> responses = new CopyOnWriteArrayList<>();
        Map<String, Exception> exceptions = new ConcurrentHashMap<>();
        for (Map.Entry<String, TransportClient> entry : transportClientMap.entrySet()) {
            String clusterAlias = entry.getKey();
            TransportClient client = entry.getValue();
            SearchRequest request = TransportCCSMultiCoordAction.createRequestForMultiCoord(searchRequest, new String[]{"so"}, clusterAlias);
            client.search(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    latch.countDown();
                    responses.add(searchResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                    exceptions.put(clusterAlias, e);
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.MINUTES));
        assertTrue(exceptions.toString(), exceptions.isEmpty());

        InternalAggregation.ReduceContext reduceContext = new InternalAggregation.ReduceContext(bigArrays, scriptService, true);
        return TransportCCSMultiCoordAction.merge(originalFrom, originalSize, responses, reduceContext);
    }
}
