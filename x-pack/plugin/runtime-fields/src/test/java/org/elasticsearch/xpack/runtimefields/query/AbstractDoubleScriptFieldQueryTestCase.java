/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.xpack.runtimefields.DoubleScript;

import static org.mockito.Mockito.mock;

public abstract class AbstractDoubleScriptFieldQueryTestCase<T extends AbstractDoubleScriptFieldQuery> extends
    AbstractScriptFieldQueryTestCase<T> {

    protected final DoubleScript.LeafFactory leafFactory = mock(DoubleScript.LeafFactory.class);

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
