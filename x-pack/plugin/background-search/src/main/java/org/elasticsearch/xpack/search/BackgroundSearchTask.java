/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.SearchTask;

public class BackgroundSearchTask extends SearchTask {

    private final SearchTask searchTask;
    private final TransportSubmitBackgroundSearchAction.BackgroundSearchStatus status;

    BackgroundSearchTask(SearchTask searchTask, TransportSubmitBackgroundSearchAction.BackgroundSearchStatus status) {
        super(searchTask.getId(), searchTask.getType(), searchTask.getAction(), null,
            searchTask.getParentTaskId(), searchTask.getHeaders());
        this.searchTask = searchTask;
        this.status = status;
    }

    @Override
    public String getDescription() {
        return searchTask.getDescription();
    }

    @Override
    public TransportSubmitBackgroundSearchAction.BackgroundSearchStatus getStatus() {
        return status;
    }
}

