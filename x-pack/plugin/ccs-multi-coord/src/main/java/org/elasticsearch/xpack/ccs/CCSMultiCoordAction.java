/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccs;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.search.SearchResponse;

public final class CCSMultiCoordAction extends Action<SearchResponse> {

    public static final CCSMultiCoordAction INSTANCE = new CCSMultiCoordAction();
    public static final String NAME = "indices:data/read/ccs_multi_coord";

    CCSMultiCoordAction() {
        super(NAME);
    }

    @Override
    public SearchResponse newResponse() {
        return new SearchResponse();
    }
}
