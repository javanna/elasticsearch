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

package org.elasticsearch.action.delete.index;

import org.elasticsearch.Version;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

public class IndexDeleteRequestTests extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            DeleteRequest deleteRequest = new DeleteRequest("alias", "type", "id");
            IndexDeleteRequest indexDeleteRequest = new IndexDeleteRequest(deleteRequest, "index");

            BytesStreamOutput out = new BytesStreamOutput();
            Version outputVersion = randomVersion();
            out.setVersion(outputVersion);
            indexDeleteRequest.writeTo(out);

            BytesStreamInput in = new BytesStreamInput(out.bytes());
            in.setVersion(outputVersion);
            IndexDeleteRequest indexDeleteRequest2 = new IndexDeleteRequest();
            indexDeleteRequest2.readFrom(in);

            assertThat(indexDeleteRequest2.type(), equalTo("type"));
            assertThat(indexDeleteRequest2.id(), equalTo("id"));
            if (outputVersion.onOrAfter(Version.V_1_4_0)) {
                assertThat(indexDeleteRequest2.index(), equalTo("alias"));
                assertThat(indexDeleteRequest2.concreteIndex(), equalTo("index"));
            } else {
                assertThat(indexDeleteRequest2.index(), equalTo("index"));
                assertThat(indexDeleteRequest2.concreteIndex(), equalTo("index"));
            }
        }
    }
}
