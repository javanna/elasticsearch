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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StringTermsTests extends InternalTermsTestCase {

    @Override
    protected InternalTerms<?, ?> createTestInstance(String name,
                                                     List<PipelineAggregator> pipelineAggregators,
                                                     Map<String, Object> metaData,
                                                     InternalAggregations aggregations,
                                                     boolean showTermDocCountError,
                                                     long docCountError) {
        BucketOrder order = BucketOrder.count(false);
        long minDocCount = 1;
        int requiredSize = 3;
        int shardSize = requiredSize + 2;
        DocValueFormat format = DocValueFormat.RAW;
        long otherDocCount = 0;
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        Set<BytesRef> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            BytesRef term = randomValueOtherThanMany(b -> terms.add(b) == false, () -> new BytesRef(randomAlphaOfLength(10)));
            int docCount = randomIntBetween(1, 100);
            buckets.add(new StringTerms.Bucket(term, docCount, aggregations, showTermDocCountError, docCountError, format));
        }
        return new StringTerms(name, order, requiredSize, minDocCount, pipelineAggregators,
                metaData, format, shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Reader<InternalTerms<?, ?>> instanceReader() {
        return StringTerms::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedStringTerms.class;
    }

    @Override
    protected InternalTerms<?, ?> mutateInstance(InternalTerms<?, ?> instance) {
        if (instance instanceof StringTerms) {
            StringTerms stringTerms = (StringTerms) instance;
            String name = stringTerms.getName();
            BucketOrder order = stringTerms.order;
            int requiredSize = stringTerms.requiredSize;
            long minDocCount = stringTerms.minDocCount;
            DocValueFormat format = stringTerms.format;
            int shardSize = stringTerms.getShardSize();
            boolean showTermDocCountError = stringTerms.showTermDocCountError;
            long otherDocCount = stringTerms.getSumOfOtherDocCounts();
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            long docCountError = stringTerms.getDocCountError();
            List<PipelineAggregator> pipelineAggregators = stringTerms.pipelineAggregators();
            Map<String, Object> metaData = stringTerms.getMetaData();
            switch (between(0, 8)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                requiredSize += between(1, 100);
                break;
            case 2:
                minDocCount += between(1, 100);
                break;
            case 3:
                shardSize += between(1, 100);
                break;
            case 4:
                showTermDocCountError = showTermDocCountError == false;
                break;
            case 5:
                otherDocCount += between(1, 100);
                break;
            case 6:
                docCountError += between(1, 100);
                break;
            case 7:
                buckets = new ArrayList<>(buckets);
                buckets.add(new StringTerms.Bucket(new BytesRef(randomAlphaOfLengthBetween(1, 10)), randomNonNegativeLong(),
                        InternalAggregations.EMPTY, showTermDocCountError, docCountError, format));
                break;
            case 8:
                if (metaData == null) {
                    metaData = new HashMap<>(1);
                } else {
                    metaData = new HashMap<>(instance.getMetaData());
                }
                metaData.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
            return new StringTerms(name, order, requiredSize, minDocCount, pipelineAggregators, metaData, format, shardSize,
                    showTermDocCountError, otherDocCount, buckets, docCountError);
        } else {
            String name = instance.getName();
            BucketOrder order = instance.order;
            int requiredSize = instance.requiredSize;
            long minDocCount = instance.minDocCount;
            List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
            Map<String, Object> metaData = instance.getMetaData();
            switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                requiredSize += between(1, 100);
                break;
            case 2:
                minDocCount += between(1, 100);
                break;
            case 3:
                if (metaData == null) {
                    metaData = new HashMap<>(1);
                } else {
                    metaData = new HashMap<>(instance.getMetaData());
                }
                metaData.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
            return new UnmappedTerms(name, order, requiredSize, minDocCount, pipelineAggregators, metaData);
        }
    }

    public void testCCSWithMinimizeRoundtrips() throws IOException {
        System.out.println("----- ccsWithMinimizeRoundtrips ------");

        //4 shards on one cluster
        String[] cluster1 = new String[]{
            "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"
        };
        String reduced1 = "AQZmaWx0ZXIHYW5zd2Vyc/8AZAEGc3Rlcm1zE2Fuc3dlcl9wZXJfcXVlc3Rpb27/AP8CAQQKAQQDcmF3GQEBQAMAAAAAAAAAAgACMTEDAAAAAAAAAAEAAjMyAwAAAAAAAAABAAIzNQMAAAAAAAAAAQABNgMAAAAAAAAAAQACNjIDAAAAAAAAAAIAAjc3AgAAAAAAAAABAAEwAgAAAAAAAAABAAExAgAAAAAAAAABAAIxMAIAAAAAAAAAAQACMTMCAAAAAAAAAAIAAjE2AgAAAAAAAAABAAIyMwIAAAAAAAAAAQACMzMCAAAAAAAAAAEAAjM3AgAAAAAAAAACAAI0NgIAAAAAAAAAAQACNDkCAAAAAAAAAAEAATUCAAAAAAAAAAEAAjUxAgAAAAAAAAABAAI1NAIAAAAAAAAAAQACNTUCAAAAAAAAAAIAAjU2AgAAAAAAAAABAAI2MAIAAAAAAAAAAQACNjcCAAAAAAAAAAIAAjY4AgAAAAAAAAACAAE4AgAAAAAAAAABAAI4MAIAAAAAAAAAAQACODcCAAAAAAAAAAEAAjk1AgAAAAAAAAABAAI5NgEAAAAAAAAAAQADMTAwAQAAAAAAAAABAAIxNQEAAAAAAAAAAQACMTcBAAAAAAAAAAIAATIBAAAAAAAAAAEAAjIyAQAAAAAAAAACAAIyOAEAAAAAAAAAAgACMjkBAAAAAAAAAAIAAjMxAQAAAAAAAAABAAIzNAEAAAAAAAAAAQACMzYBAAAAAAAAAAEAAjM4AQAAAAAAAAABAAI0MQEAAAAAAAAAAQACNDQBAAAAAAAAAAIAAjUyAQAAAAAAAAACAAI1MwEAAAAAAAAAAQACNTcBAAAAAAAAAAEAAjU4AQAAAAAAAAABAAI1OQEAAAAAAAAAAQACNjMBAAAAAAAAAAEAAjY2AQAAAAAAAAABAAE3AQAAAAAAAAABAAI3MAEAAAAAAAAAAgACNzIBAAAAAAAAAAEAAjc0AQAAAAAAAAABAAI3NgEAAAAAAAAAAQACNzgBAAAAAAAAAAEAAjgyAQAAAAAAAAABAAI4MwEAAAAAAAAAAgACODUBAAAAAAAAAAEAAjg2AQAAAAAAAAACAAI4OQEAAAAAAAAAAgABOQEAAAAAAAAAAgACOTIBAAAAAAAAAAEAAjk0AQAAAAAAAAACAAI5OAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        System.out.println("first cluster:");
        reduce(cluster1, reduced1, false);

        //4 shards on the other cluster cluster
        String[] cluster2 = new String[]{
            "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",
        };
        String reduced2 = "AQZmaWx0ZXIHYW5zd2Vyc/8AZAEGc3Rlcm1zE2Fuc3dlcl9wZXJfcXVlc3Rpb27/AP8CAQQKAQADcmF3GQEAQAMAAAAAAAAAAAACMTEDAAAAAAAAAAAAAjMyAwAAAAAAAAAAAAIzNQMAAAAAAAAAAAABNgMAAAAAAAAAAAACNjIDAAAAAAAAAAAAAjc3AgAAAAAAAAAAAAEwAgAAAAAAAAAAAAExAgAAAAAAAAAAAAIxMAIAAAAAAAAAAAACMTMCAAAAAAAAAAAAAjE2AgAAAAAAAAAAAAIyMwIAAAAAAAAAAAACMzMCAAAAAAAAAAAAAjM3AgAAAAAAAAAAAAI0NgIAAAAAAAAAAAACNDkCAAAAAAAAAAAAATUCAAAAAAAAAAAAAjUxAgAAAAAAAAAAAAI1NAIAAAAAAAAAAAACNTUCAAAAAAAAAAAAAjU2AgAAAAAAAAAAAAI2MAIAAAAAAAAAAAACNjcCAAAAAAAAAAAAAjY4AgAAAAAAAAAAAAE4AgAAAAAAAAAAAAI4MAIAAAAAAAAAAAACODcCAAAAAAAAAAAAAjk1AgAAAAAAAAAAAAI5NgIAAAAAAAAAAAACOTgBAAAAAAAAAAAAAzEwMAEAAAAAAAAAAAACMTUBAAAAAAAAAAAAAjE3AQAAAAAAAAAAAAEyAQAAAAAAAAAAAAIyMgEAAAAAAAAAAAACMjgBAAAAAAAAAAAAAjI5AQAAAAAAAAAAAAIzMQEAAAAAAAAAAAACMzQBAAAAAAAAAAAAAjM2AQAAAAAAAAAAAAIzOAEAAAAAAAAAAAACNDEBAAAAAAAAAAAAAjQ0AQAAAAAAAAAAAAI1MgEAAAAAAAAAAAACNTMBAAAAAAAAAAAAAjU3AQAAAAAAAAAAAAI1OAEAAAAAAAAAAAACNTkBAAAAAAAAAAAAAjYzAQAAAAAAAAAAAAI2NgEAAAAAAAAAAAABNwEAAAAAAAAAAAACNzABAAAAAAAAAAAAAjcyAQAAAAAAAAAAAAI3NAEAAAAAAAAAAAACNzYBAAAAAAAAAAAAAjc4AQAAAAAAAAAAAAI4MgEAAAAAAAAAAAACODMBAAAAAAAAAAAAAjg1AQAAAAAAAAAAAAI4NgEAAAAAAAAAAAACODkBAAAAAAAAAAAAATkBAAAAAAAAAAAAAjkyAQAAAAAAAAAAAAI5NAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        System.out.println("second cluster:");
        reduce(cluster2, reduced2, false);

        String expectedFinallyReduced = "AQZmaWx0ZXIHYW5zd2Vyc/8AyAEBBnN0ZXJtcxNhbnN3ZXJfcGVyX3F1ZXN0aW9u/wD/AgEECgEGA3JhdxkBlAEKBgAAAAAAAAACAAIxMQYAAAAAAAAAAgACMzIGAAAAAAAAAAIAAjM1BgAAAAAAAAACAAE2BgAAAAAAAAACAAI2MgYAAAAAAAAAAgACNzcEAAAAAAAAAAIAATAEAAAAAAAAAAIAATEEAAAAAAAAAAIAAjEwBAAAAAAAAAACAAIxMwAAAAAAAAA=";
        System.out.println("final reduction:");
        reduce(new String[]{reduced1, reduced2}, expectedFinallyReduced, true);
    }

    public void testCCSWithoutMinimizeRoundtrips() throws IOException {
        System.out.println("----- ccsWithoutMinimizeRoundtrips ------");

        //8 shard level responses get reduced together in one go on the coordinating node
        String[] responses =  new String[] {
            "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"
        };

        String reduced = "AQZmaWx0ZXIHYW5zd2Vyc/8AyAEBBnN0ZXJtcxNhbnN3ZXJfcGVyX3F1ZXN0aW9u/wD/AgEECgEEA3JhdxkBlAEKBgAAAAAAAAACAAIxMQYAAAAAAAAAAQACMzIGAAAAAAAAAAEAAjM1BgAAAAAAAAABAAE2BgAAAAAAAAABAAI2MgYAAAAAAAAAAgACNzcEAAAAAAAAAAEAATAEAAAAAAAAAAEAATEEAAAAAAAAAAEAAjEwBAAAAAAAAAABAAIxMwAAAAAAAAA=";

        reduce(responses, reduced, true);
    }

    private void reduce(String[] base64Aggs, String expectedResult, boolean finalReduce) throws IOException {
        List<InternalAggregations> list = new ArrayList<>();
        for (int i = 0; i < base64Aggs.length; i++) {
            String response = base64Aggs[i];
            InternalAggregations aggs = readAggs(response);
            list.add(aggs);
            System.out.println("response " + i + ": " + Strings.toString(aggs, true, true));
        }

        InternalAggregations reducedAggs = InternalAggregations.reduce(list, new InternalAggregation.ReduceContext(null, null, finalReduce));
        System.out.println("reduced: " + Strings.toString(reducedAggs, true, true));

        InternalAggregations result = readAggs(expectedResult);
        assertEquals(result, reducedAggs);
    }

    private InternalAggregations readAggs(String base64Response) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(base64Response);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), getNamedWriteableRegistry())) {
            return InternalAggregations.readAggregations(in);
        }
    }
}
