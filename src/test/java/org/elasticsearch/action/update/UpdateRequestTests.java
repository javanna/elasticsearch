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

package org.elasticsearch.action.update;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class UpdateRequestTests extends ElasticsearchTestCase {

    @Test
    public void testUpdateRequest() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "type", "1");
        // simple script
        request.source(XContentFactory.jsonBuilder().startObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));

        // script with params
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .field("script", "script1")
                .startObject("params").field("param1", "value1").endObject()
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));

        // script with params and upsert
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        Map<String, Object> upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        // script with doc
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("doc").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .endObject());
        Map<String, Object> doc = request.doc().sourceAsMap();
        assertThat(doc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) doc.get("compound")).get("field2").toString(), equalTo("value2"));
    }

    @Test
    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            UpdateRequest updateRequest = new UpdateRequest("alias", "type", "id");
            boolean setConcreteIndex = randomBoolean();
            if (setConcreteIndex) {
                updateRequest.concreteIndex("index");
            }

            BytesStreamOutput out = new BytesStreamOutput();
            Version outputVersion = randomVersion();
            out.setVersion(outputVersion);
            updateRequest.writeTo(out);

            BytesStreamInput in = new BytesStreamInput(out.bytes());
            in.setVersion(outputVersion);
            UpdateRequest updateRequest2 = new UpdateRequest();
            updateRequest2.readFrom(in);

            assertThat(updateRequest2.type(), equalTo("type"));
            assertThat(updateRequest2.id(), equalTo("id"));
            if (outputVersion.onOrAfter(Version.V_1_4_0)) {
                assertThat(updateRequest2.index(), equalTo("alias"));
                if (setConcreteIndex) {
                    assertThat(updateRequest2.concreteIndex(), equalTo("index"));
                } else {
                    assertThat(updateRequest2.concreteIndex(), nullValue());
                }
            } else {
                if (setConcreteIndex) {
                    //when we have a concrete index we serialize it as the only index
                    assertThat(updateRequest2.index(), equalTo("index"));
                    assertThat(updateRequest2.concreteIndex(), equalTo("index"));
                } else {
                    //client case: when we don't have a concrete index we serialize the original index
                    //which will get read as concrete one as well but resolved on the coordinating node
                    assertThat(updateRequest2.index(), equalTo("alias"));
                    assertThat(updateRequest2.concreteIndex(), equalTo("alias"));
                }
            }
        }
    }

}
