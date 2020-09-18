/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Collections;

public class SparseVectorFieldTypeTests extends FieldTypeTestCase {

    @Override
    protected boolean hasConfigurableDocValues() {
        return false;
    }

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
    }

    public void testDocValuesDisabled() {
        MappedFieldType fieldType = createDefaultFieldType();
        assertFalse(fieldType.hasDocValues());
        expectThrows(IllegalArgumentException.class, () -> fieldType.fielddataBuilder("index", null));
    }

    public void testIsNotAggregatable() {
        MappedFieldType fieldType = createDefaultFieldType();
        assertFalse(fieldType.isAggregatable());
    }
}
