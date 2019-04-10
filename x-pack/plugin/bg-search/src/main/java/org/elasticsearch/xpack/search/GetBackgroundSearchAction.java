/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.stream.Writeable;

public class GetBackgroundSearchAction extends Action<GetBackgroundSearchResponse>  {

    public static final GetBackgroundSearchAction INSTANCE = new GetBackgroundSearchAction();
    //TODO is this the right action category?
    public static final String NAME = "indices:data/read/xpack/get/search";

    private GetBackgroundSearchAction() {
        super(NAME);
    }

    @Override
    public GetBackgroundSearchResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<GetBackgroundSearchResponse> getResponseReader() {
        return GetBackgroundSearchResponse::new;
    }
}
