/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ObjectFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;

public class ObjectRuntimeFieldTests extends MapperServiceTestCase {

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        if (context == ObjectFieldScript.CONTEXT) {
            Consumer<TestObjectFieldScript> executor = objectScript(script.getIdOrCode());
            return (T) (ObjectFieldScript.Factory) (fieldName, params, searchLookup) -> ctx -> new TestObjectFieldScript(
                fieldName,
                params,
                searchLookup,
                ctx,
                executor
            );
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

    private static class TestObjectFieldScript extends ObjectFieldScript {

        private final Consumer<TestObjectFieldScript> executor;

        TestObjectFieldScript(
            String fieldName,
            Map<String, Object> params,
            SearchLookup searchLookup,
            LeafReaderContext ctx,
            Consumer<TestObjectFieldScript> executor
        ) {
            super(fieldName, params, searchLookup, ctx);
            this.executor = executor;
        }

        @Override
        public void execute() {
            executor.accept(this);
        }

        public List<Object> extractFromSource(String path) {
            return super.extractFromSource(path);
        }
    }

    protected Consumer<TestObjectFieldScript> objectScript(String scriptName) {
        if (scriptName.equals("split-str-long")) {
            return script -> {
                List<Object> values = script.extractFromSource("field");
                String input = values.get(0).toString();
                String[] parts = input.split(" ");
                script.emit("str", parts[0]);
                script.emit("long", parts[1]);
            };
        }
        return script -> {};
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

    public void testSubFieldAccess() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "object");
            b.field("script", "split-str-long");
            b.startObject("fields");
            b.startObject("str").field("type", "keyword").endObject();
            b.startObject("long").field("type", "long").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        ParsedDocument doc1 = mapperService.documentMapper().parse(source(b -> b.field("field", "foo 1")));
        ParsedDocument doc2 = mapperService.documentMapper().parse(source(b -> b.field("field", "bar 2")));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(Arrays.asList(doc1.rootDoc(), doc2.rootDoc())), reader -> {

            SearchLookup searchLookup = new SearchLookup(
                mapperService::fieldType,
                (mft, lookupSupplier) -> mft.fielddataBuilder("test", lookupSupplier).build(null, null)
            );

            LeafSearchLookup leafSearchLookup = searchLookup.getLeafSearchLookup(reader.leaves().get(0));

            leafSearchLookup.setDocument(0);
            assertEquals("foo", leafSearchLookup.doc().get("obj.str").get(0));
            assertEquals(1L, leafSearchLookup.doc().get("obj.long").get(0));

            leafSearchLookup.setDocument(1);
            assertEquals("bar", leafSearchLookup.doc().get("obj.str").get(0));
            assertEquals(2L, leafSearchLookup.doc().get("obj.long").get(0));

        });

    }

}
