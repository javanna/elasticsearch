/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.io.InputStream;

public class GetBackgroundSearchResponse extends ActionResponse implements ToXContentObject {

    private final int totalShards;
    private final int completedOperations;
    private final InternalAggregations partialAggs;
    private final BytesReference response;

    GetBackgroundSearchResponse(int totalShards, int completedOperations, InternalAggregations partialAggs) {
        this.totalShards = totalShards;
        this.completedOperations = completedOperations;
        this.partialAggs = partialAggs;
        this.response = null;
    }

    GetBackgroundSearchResponse(BytesReference response) {
        this.totalShards = -1;
        this.completedOperations = -1;
        this.partialAggs = null;
        this.response = response;
    }

    GetBackgroundSearchResponse(StreamInput in) throws IOException {
        this.totalShards = in.readVInt();
        this.completedOperations = in.readVInt();
        this.partialAggs = in.readOptionalWriteable(InternalAggregations::new);
        this.response = in.readOptionalBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalShards);
        out.writeVInt(completedOperations);
        out.writeOptionalWriteable(partialAggs);
        out.writeOptionalBytesReference(response);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (response == null) {
            builder.field("total_shards", totalShards);
            builder.field("completed_operations", completedOperations);
            if (partialAggs != null) {
                partialAggs.toXContent(builder, params);
            }
        } else {
            try (InputStream stream = response.streamInput()) {
                builder.field("response");
                builder.rawValue(stream, XContentHelper.xContentType(response));
            }
        }
        builder.endObject();
        return builder;
    }
}
