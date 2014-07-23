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
package org.elasticsearch.action.index;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
  */
public class IndexRequestTests extends ElasticsearchTestCase {

    @Test
    public void testIndexRequestOpTypeFromString() throws Exception {
        String create = "create";
        String index = "index";
        String createUpper = "CREATE";
        String indexUpper = "INDEX";

        assertThat(IndexRequest.OpType.fromString(create), equalTo(IndexRequest.OpType.CREATE));
        assertThat(IndexRequest.OpType.fromString(index), equalTo(IndexRequest.OpType.INDEX));
        assertThat(IndexRequest.OpType.fromString(createUpper), equalTo(IndexRequest.OpType.CREATE));
        assertThat(IndexRequest.OpType.fromString(indexUpper), equalTo(IndexRequest.OpType.INDEX));
    }

    @Test(expected= ElasticsearchIllegalArgumentException.class)
    public void testReadBogusString(){
        String foobar = "foobar";
        IndexRequest.OpType.fromString(foobar);
    }

    @Test
    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            IndexRequest indexRequest = new IndexRequest("alias", "type", "id");
            boolean setConcreteIndex = randomBoolean();
            if (setConcreteIndex) {
                indexRequest.concreteIndex("index");
            }

            BytesStreamOutput out = new BytesStreamOutput();
            Version outputVersion = randomVersion();
            out.setVersion(outputVersion);
            indexRequest.writeTo(out);

            BytesStreamInput in = new BytesStreamInput(out.bytes());
            in.setVersion(outputVersion);
            IndexRequest indexRequest2 = new IndexRequest();
            indexRequest2.readFrom(in);

            assertThat(indexRequest2.type(), equalTo("type"));
            assertThat(indexRequest2.id(), equalTo("id"));
            if (outputVersion.onOrAfter(Version.V_1_4_0)) {
                assertThat(indexRequest2.index(), equalTo("alias"));
                if (setConcreteIndex) {
                    assertThat(indexRequest2.concreteIndex(), equalTo("index"));
                } else {
                    assertThat(indexRequest2.concreteIndex(), nullValue());
                }
            } else {
                if (setConcreteIndex) {
                    //when we have a concrete index we serialize it as the only index
                    assertThat(indexRequest2.index(), equalTo("index"));
                    assertThat(indexRequest2.concreteIndex(), equalTo("index"));
                } else {
                    //client case: when we don't have a concrete index we serialize the original index
                    //which will get read as concrete one as well but resolved on the coordinating node
                    assertThat(indexRequest2.index(), equalTo("alias"));
                    assertThat(indexRequest2.concreteIndex(), equalTo("alias"));
                }
            }
        }
    }
}
