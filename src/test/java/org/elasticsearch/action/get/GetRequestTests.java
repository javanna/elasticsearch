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

package org.elasticsearch.action.get;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

public class GetRequestTests extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            GetRequest getRequest = new GetRequest("alias", "type", "id");
            boolean setConcreteIndex = randomBoolean();
            if (setConcreteIndex) {
                getRequest.concreteIndex("index");
            }

            BytesStreamOutput out = new BytesStreamOutput();
            Version outputVersion = randomVersion();
            out.setVersion(outputVersion);
            getRequest.writeTo(out);

            BytesStreamInput in = new BytesStreamInput(out.bytes());
            in.setVersion(outputVersion);
            GetRequest getRequest2 = new GetRequest();
            getRequest2.readFrom(in);

            assertThat(getRequest2.type(), equalTo("type"));
            assertThat(getRequest2.id(), equalTo("id"));
            if (outputVersion.onOrAfter(Version.V_1_4_0)) {
                assertThat(getRequest2.index(), equalTo("alias"));
                if (setConcreteIndex) {
                    assertThat(getRequest2.concreteIndex(), equalTo("index"));
                } else {
                    assertThat(getRequest2.concreteIndex(), nullValue());
                }
            } else {
                if (setConcreteIndex) {
                    //when we have a concrete index we serialize it as the only index
                    assertThat(getRequest2.index(), equalTo("index"));
                    assertThat(getRequest2.concreteIndex(), equalTo("index"));
                } else {
                    //client case: when we don't have a concrete index we serialize the original index
                    //which will get read as concrete one as well but resolved on the coordinating node
                    assertThat(getRequest2.index(), equalTo("alias"));
                    assertThat(getRequest2.concreteIndex(), equalTo("alias"));
                }
            }
        }
    }
}
