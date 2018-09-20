/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.search.SearchRequest;

import java.io.IOException;

public final class SearchRequestConverter {

    private SearchRequestConverter() {

    }

    public static Request search(SearchRequest searchRequest) throws IOException {
        return RequestConverters.search(searchRequest);
    }
}
