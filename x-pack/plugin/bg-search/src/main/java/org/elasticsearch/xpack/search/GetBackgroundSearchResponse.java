/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class GetBackgroundSearchResponse extends ActionResponse implements ToXContentObject {

    GetBackgroundSearchResponse() {

    }

    GetBackgroundSearchResponse(StreamInput in) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}
