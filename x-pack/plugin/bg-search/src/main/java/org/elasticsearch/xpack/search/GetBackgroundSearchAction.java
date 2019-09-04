/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.Writeable;

public class GetBackgroundSearchAction extends ActionType<GetBackgroundSearchResponse> {

    public static final GetBackgroundSearchAction INSTANCE = new GetBackgroundSearchAction();
    //TODO is this the right action category?
    public static final String NAME = "indices:data/read/xpack/get/search";

    private GetBackgroundSearchAction() {
        super(NAME, GetBackgroundSearchResponse::new);
    }

    @Override
    public Writeable.Reader<GetBackgroundSearchResponse> getResponseReader() {
        return GetBackgroundSearchResponse::new;
    }
}
