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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestTests;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class RolloverRequestTests extends ESTestCase {

    private NamedWriteableRegistry writeableRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        writeableRegistry = new NamedWriteableRegistry(new IndicesModule(Collections.emptyList()).getNamedWriteables());
    }

    public void testConditionsParsing() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("conditions")
                    .field("max_age", "10d")
                    .field("max_docs", 100)
                    .field("max_size", "45gb")
                .endObject()
            .endObject();
        request.fromXContent(createParser(builder));
        Set<Condition> conditions = request.getConditions();
        assertThat(conditions.size(), equalTo(3));
        for (Condition condition : conditions) {
            if (condition instanceof MaxAgeCondition) {
                MaxAgeCondition maxAgeCondition = (MaxAgeCondition) condition;
                assertThat(maxAgeCondition.value.getMillis(), equalTo(TimeValue.timeValueHours(24 * 10).getMillis()));
            } else if (condition instanceof MaxDocsCondition) {
                MaxDocsCondition maxDocsCondition = (MaxDocsCondition) condition;
                assertThat(maxDocsCondition.value, equalTo(100L));
            } else if (condition instanceof MaxSizeCondition) {
                MaxSizeCondition maxSizeCondition = (MaxSizeCondition) condition;
                assertThat(maxSizeCondition.value.getBytes(), equalTo(ByteSizeUnit.GB.toBytes(45)));
            } else {
                fail("unexpected condition " + condition);
            }
        }
    }

    public void testParsingWithIndexSettings() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("conditions")
                    .field("max_age", "10d")
                    .field("max_docs", 100)
                .endObject()
                .startObject("mappings")
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("field1")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("settings")
                    .field("number_of_shards", 10)
                .endObject()
                .startObject("aliases")
                    .startObject("alias1").endObject()
                .endObject()
            .endObject();
        request.fromXContent(createParser(builder));
        Set<Condition> conditions = request.getConditions();
        assertThat(conditions.size(), equalTo(2));
        assertThat(request.getCreateIndexRequest().mappings().size(), equalTo(1));
        assertThat(request.getCreateIndexRequest().aliases().size(), equalTo(1));
        assertThat(request.getCreateIndexRequest().settings().getAsInt("number_of_shards", 0), equalTo(10));
    }

    public void testSerialize() throws Exception {
        RolloverRequest originalRequest = new RolloverRequest("alias-index", "new-index-name");
        originalRequest.addMaxIndexDocsCondition(randomNonNegativeLong());
        originalRequest.addMaxIndexAgeCondition(TimeValue.timeValueNanos(randomNonNegativeLong()));
        originalRequest.addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong()));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = new NamedWriteableAwareStreamInput(bytes.streamInput(), writeableRegistry)) {
                RolloverRequest cloneRequest = new RolloverRequest();
                cloneRequest.readFrom(in);
                assertThat(cloneRequest.getNewIndexName(), equalTo(originalRequest.getNewIndexName()));
                assertThat(cloneRequest.getAlias(), equalTo(originalRequest.getAlias()));

                List<String> originalConditions = originalRequest.getConditions().stream()
                    .map(Condition::toString)
                    .sorted()
                    .collect(Collectors.toList());

                List<String> cloneConditions = cloneRequest.getConditions().stream()
                    .map(Condition::toString)
                    .sorted()
                    .collect(Collectors.toList());

                assertThat(originalConditions, equalTo(cloneConditions));
            }
        }
    }

    public void testToAndFromXContent() throws IOException {
        RolloverRequest rolloverRequest = createTestItem();

        final XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(rolloverRequest, xContentType, EMPTY_PARAMS, humanReadable);

        RolloverRequest parsedRolloverRequest = new RolloverRequest();
        parsedRolloverRequest.fromXContent(createParser(xContentType.xContent(), originalBytes));

        CreateIndexRequest createIndexRequest = rolloverRequest.getCreateIndexRequest();
        CreateIndexRequest parsedCreateIndexRequest = parsedRolloverRequest.getCreateIndexRequest();
        CreateIndexRequestTests.assertMappingsEqual(createIndexRequest.mappings(), parsedCreateIndexRequest.mappings());
        CreateIndexRequestTests.assertAliasesEqual(createIndexRequest.aliases(), parsedCreateIndexRequest.aliases());
        assertEquals(createIndexRequest.settings(), parsedCreateIndexRequest.settings());

        assertEquals(rolloverRequest.getConditions(), parsedRolloverRequest.getConditions());

        BytesReference finalBytes = toShuffledXContent(parsedRolloverRequest, xContentType, EMPTY_PARAMS, humanReadable);
        ElasticsearchAssertions.assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }

    public void testUnknownFields() throws IOException {
        final RolloverRequest request = new RolloverRequest();
        XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject();
        {
            builder.startObject("conditions");
            builder.field("max_age", "10d");
            builder.endObject();
        }
        builder.endObject();
        BytesReference mutated = XContentTestUtils.insertRandomFields(xContentType, builder.bytes(), null, random());
        expectThrows(ParsingException.class, () -> request.fromXContent(createParser(xContentType.xContent(), mutated)));
    }

    public void testAddConditions() {
        RolloverRequest rolloverRequest = new RolloverRequest();
        Consumer<RolloverRequest> rolloverRequestConsumer = randomFrom(conditionsGenerator);
        rolloverRequestConsumer.accept(rolloverRequest);
        expectThrows(IllegalArgumentException.class, () -> rolloverRequestConsumer.accept(rolloverRequest));
    }

    public void testValidation() {
        RolloverRequest rolloverRequest = new RolloverRequest();
        assertNotNull(rolloverRequest.getCreateIndexRequest());
        expectThrows(NullPointerException.class, () -> rolloverRequest.setCreateIndexRequest(null));

        ActionRequestValidationException validationException = rolloverRequest.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertEquals("index alias is missing", validationException.validationErrors().get(0));
    }

    private static List<Consumer<RolloverRequest>> conditionsGenerator = new ArrayList<>();
    static {
        conditionsGenerator.add((request) -> request.addMaxIndexDocsCondition(randomNonNegativeLong()));
        conditionsGenerator.add((request) -> request.addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong())));
        conditionsGenerator.add((request) -> request.addMaxIndexAgeCondition(new TimeValue(randomNonNegativeLong())));
    }

    private static RolloverRequest createTestItem() throws IOException {
        RolloverRequest rolloverRequest = new RolloverRequest();
        if (randomBoolean()) {
            rolloverRequest.setCreateIndexRequest(RandomCreateIndexGenerator.randomCreateIndexRequest());
        }
        int numConditions = randomIntBetween(0, 3);
        Set<Consumer<RolloverRequest>> consumers = new HashSet<>();
        for (int i = 0; i < numConditions; i++) {
            while (consumers.add(randomFrom(conditionsGenerator)) == false) {
            }
        }
        for (Consumer<RolloverRequest> consumer : consumers) {
            consumer.accept(rolloverRequest);
        }
        return rolloverRequest;
    }
}
