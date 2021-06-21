/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class ScriptRuntimeField implements RuntimeField {

    protected final String name;
    protected final ToXContent toXContent;
    protected final MappedFieldType fieldType;

    public ScriptRuntimeField(String name, MappedFieldType fieldType, ToXContent toXContent) {
        this.name = name;
        this.toXContent = toXContent;
        this.fieldType = fieldType;
    }

    @Override
    public String simpleName() {
        return name;
    }

    @Override
    public String typeName() {
        return fieldType.typeName();
    }

    @Override
    public final Collection<MappedFieldType> asMappedFieldTypes() {
        return Collections.singleton(fieldType);
    }

    @Override
    public final void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        toXContent.toXContent(builder, params);
    }
}
