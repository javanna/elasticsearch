/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ObjectFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TestObjectRuntimeField extends MapperServiceTestCase {

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        if (context == ObjectFieldScript.CONTEXT) {
            return (T) (ObjectFieldScript.Factory) (fieldName, params, searchLookup) -> ctx -> new ObjectFieldScript(
                fieldName,
                params,
                searchLookup,
                ctx
            ) {
                @Override
                public void execute() {

                }
            };
        }
        if (context == LongFieldScript.CONTEXT) {
            return (T) (LongFieldScript.Factory) (field, params, lookup) -> ctx -> new LongFieldScript(field, params, lookup, ctx) {
                @Override
                public void execute() {

                }
            };
        }
        throw new UnsupportedOperationException("Unknown context " + context.name);
    }

    public void testObjectDefinition() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "object");
            b.field("script", "dummy");
            b.startObject("fields");
            b.startObject("long-subfield").field("type", "long").endObject();
            b.startObject("str-subfield").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        MappedFieldType longSubfield = mapperService.mappingLookup().getFieldType("obj.long-subfield");
        assertEquals("obj.long-subfield", longSubfield.name());

        RuntimeField rf = mapperService.mappingLookup().getMapping().getRoot().getRuntimeField("obj");
        assertEquals(
            "{\"obj\":{\"type\":\"object\",\"script\":{\"source\":\"dummy\",\"lang\":\"painless\"}," +
                "\"fields\":{\"long-subfield\":{\"type\":\"long\"},\"str-subfield\":{\"type\":\"keyword\"}}}}",
            Strings.toString(rf)
        );
    }

    public void testScriptOnSubFieldThrowsError() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(runtimeMapping(b -> {
            b.startObject("obj");
            b.field("type", "object");
            b.field("script", "dummy");
            b.startObject("fields");
            b.startObject("long").field("type", "long").field("script", "dummy").endObject();
            b.endObject();
            b.endObject();
        })));

        assertThat(e.getMessage(), containsString("Cannot use [script] parameter on sub-field [long] of object field [obj]"));
    }

}
