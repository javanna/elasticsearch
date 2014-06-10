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
package org.elasticsearch.action.bench;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to abort a specified benchmark
 */
public class AbortBenchmarkRequest extends AcknowledgedRequest<AbortBenchmarkRequest> {

    private String[] benchmarkNames = Strings.EMPTY_ARRAY;

    public AbortBenchmarkRequest() { }

    public AbortBenchmarkRequest(String... benchmarkNames) {
        this.benchmarkNames = benchmarkNames;
    }

    public void benchmarkNames(String... benchmarkNames) {
        this.benchmarkNames = benchmarkNames;
    }

    public String[] benchmarkNames() {
        return benchmarkNames;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (benchmarkNames == null || benchmarkNames.length == 0) {
            return ValidateActions.addValidationError("benchmarkNames must not be null or empty", null);
        }
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        benchmarkNames = in.readStringArray();
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(benchmarkNames);
        writeTimeout(out);
    }
}
