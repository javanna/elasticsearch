/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public final class BackgroundSearchResponse extends ActionResponse implements ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) {
        //this is empty on purpose, the results are better retrieved from the status which is always returned.
        return builder;
    }
}
