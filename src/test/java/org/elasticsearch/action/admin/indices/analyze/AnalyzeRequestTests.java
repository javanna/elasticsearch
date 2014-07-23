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

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

public class AnalyzeRequestTests extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            AnalyzeRequest analyzeRequest = new AnalyzeRequest("alias", "text");
            boolean setConcreteIndex = randomBoolean();
            if (setConcreteIndex) {
                analyzeRequest.concreteIndex("index");
            }

            BytesStreamOutput out = new BytesStreamOutput();
            Version outputVersion = randomVersion();
            out.setVersion(outputVersion);
            analyzeRequest.writeTo(out);

            BytesStreamInput in = new BytesStreamInput(out.bytes());
            in.setVersion(outputVersion);
            AnalyzeRequest analyzeRequest2 = new AnalyzeRequest();
            analyzeRequest2.readFrom(in);

            assertThat(analyzeRequest2.text(), equalTo("text"));
            if (outputVersion.onOrAfter(Version.V_1_4_0)) {
                assertThat(analyzeRequest2.index(), equalTo("alias"));
                if (setConcreteIndex) {
                    assertThat(analyzeRequest2.concreteIndex(), equalTo("index"));
                } else {
                    assertThat(analyzeRequest2.concreteIndex(), nullValue());
                }
            } else {
                if (setConcreteIndex) {
                    //when we have a concrete index we serialize it as the only index
                    assertThat(analyzeRequest2.index(), equalTo("index"));
                    assertThat(analyzeRequest2.concreteIndex(), equalTo("index"));
                } else {
                    //client case: when we don't have a concrete index we serialize the original index
                    //which will get read as concrete one as well but resolved on the coordinating node
                    assertThat(analyzeRequest2.index(), equalTo("alias"));
                    assertThat(analyzeRequest2.concreteIndex(), equalTo("alias"));
                }
            }
        }
    }
}
