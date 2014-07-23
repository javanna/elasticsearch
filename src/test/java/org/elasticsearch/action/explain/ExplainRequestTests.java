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

package org.elasticsearch.action.explain;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

public class ExplainRequestTests extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            ExplainRequest explainRequest = new ExplainRequest("alias", "type", "id");
            boolean setConcreteIndex = randomBoolean();
            if (setConcreteIndex) {
                explainRequest.concreteIndex("index");
            }

            BytesStreamOutput out = new BytesStreamOutput();
            Version outputVersion = randomVersion();
            out.setVersion(outputVersion);
            explainRequest.writeTo(out);

            BytesStreamInput in = new BytesStreamInput(out.bytes());
            in.setVersion(outputVersion);
            ExplainRequest explainRequest2 = new ExplainRequest();
            explainRequest2.readFrom(in);

            assertThat(explainRequest2.type(), equalTo("type"));
            assertThat(explainRequest2.id(), equalTo("id"));
            if (outputVersion.onOrAfter(Version.V_1_4_0)) {
                assertThat(explainRequest2.index(), equalTo("alias"));
                if (setConcreteIndex) {
                    assertThat(explainRequest2.concreteIndex(), equalTo("index"));
                } else {
                    assertThat(explainRequest2.concreteIndex(), nullValue());
                }
            } else {
                if (setConcreteIndex) {
                    //when we have a concrete index we serialize it as the only index
                    assertThat(explainRequest2.index(), equalTo("index"));
                    assertThat(explainRequest2.concreteIndex(), equalTo("index"));
                } else {
                    //client case: when we don't have a concrete index we serialize the original index
                    //which will get read as concrete one as well but resolved on the coordinating node
                    assertThat(explainRequest2.index(), equalTo("alias"));
                    assertThat(explainRequest2.concreteIndex(), equalTo("alias"));
                }
            }
        }
    }
}
