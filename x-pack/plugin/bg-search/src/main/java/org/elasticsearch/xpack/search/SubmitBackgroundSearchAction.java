/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.Writeable;

public final class SubmitBackgroundSearchAction extends ActionType<SubmitBackgroundSearchResponse> {

    public static final SubmitBackgroundSearchAction INSTANCE = new SubmitBackgroundSearchAction();
    //TODO is this the right action category?
    public static final String NAME = "indices:data/read/xpack/submit/search";

    private SubmitBackgroundSearchAction() {
        super(NAME, SubmitBackgroundSearchResponse::new);
    }

    @Override
    public Writeable.Reader<SubmitBackgroundSearchResponse> getResponseReader() {
        return SubmitBackgroundSearchResponse::new;
    }
}
