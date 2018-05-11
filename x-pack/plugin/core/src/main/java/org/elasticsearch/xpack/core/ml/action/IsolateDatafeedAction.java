/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.MLMetadataField;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

/**
 * An internal action that isolates a datafeed.
 * Datafeed isolation is effectively disconnecting a running datafeed task
 * from its job, i.e. even though the datafeed performs a search, the retrieved
 * data is not sent to the job, etc. As stopping a datafeed cannot always happen
 * instantaneously (e.g. cannot cancel an ongoing search), isolating a datafeed
 * task ensures the current datafeed task can complete inconsequentially while
 * the datafeed persistent task may be stopped or reassigned on another node.
 */
public class    IsolateDatafeedAction
        extends Action<IsolateDatafeedAction.Request, IsolateDatafeedAction.Response, IsolateDatafeedAction.RequestBuilder> {

    public static final IsolateDatafeedAction INSTANCE = new IsolateDatafeedAction();
    public static final String NAME = "cluster:internal/xpack/ml/datafeed/isolate";

    private IsolateDatafeedAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, datafeedId) -> request.datafeedId = datafeedId, DatafeedConfig.ID);
        }

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (datafeedId != null) {
                request.datafeedId = datafeedId;
            }
            return request;
        }

        private String datafeedId;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        public Request() {
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        @Override
        public boolean match(Task task) {
            String expectedDescription = MLMetadataField.datafeedTaskId(datafeedId);
            if (task instanceof StartDatafeedAction.DatafeedTaskMatcher && expectedDescription.equals(task.getDescription())){
                return true;
            }
            return false;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(datafeedId, other.datafeedId);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable {

        private boolean isolated;

        public Response(boolean isolated) {
            super(null, null);
            this.isolated = isolated;
        }

        public Response(StreamInput in) throws IOException {
            super(null, null);
            readFrom(in);
        }

        public Response() {
            super(null, null);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            isolated = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(isolated);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, IsolateDatafeedAction action) {
            super(client, action, new Request());
        }
    }

}
