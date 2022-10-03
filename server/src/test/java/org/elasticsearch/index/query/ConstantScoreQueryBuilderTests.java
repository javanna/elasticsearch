/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import io.netty.util.Constant;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.search.SearchModule.INDICES_MAX_NESTED_DEPTH_SETTING;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsString;

public class ConstantScoreQueryBuilderTests extends AbstractQueryTestCase<ConstantScoreQueryBuilder> {
    /**
     * @return a {@link ConstantScoreQueryBuilder} with random boost between 0.1f and 2.0f
     */
    @Override
    protected ConstantScoreQueryBuilder doCreateTestQueryBuilder() {
        return new ConstantScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));
    }

    @Override
    protected void doAssertLuceneQuery(ConstantScoreQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        Query innerQuery = queryBuilder.innerQuery().rewrite(context).toQuery(context);
        if (innerQuery == null) {
            assertThat(query, nullValue());
        } else if (innerQuery instanceof MatchNoDocsQuery) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            assertThat(constantScoreQuery.getQuery(), instanceOf(innerQuery.getClass()));
        }
    }

    /**
     * test that missing "filter" element causes {@link ParsingException}
     */
    public void testFilterElement() throws IOException {
        String queryString = "{ \"" + ConstantScoreQueryBuilder.NAME + "\" : {} }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryString));
        assertThat(e.getMessage(), containsString("requires a 'filter' element"));
    }

    /**
     * test that "filter" does not accept an array of queries, throws {@link ParsingException}
     */
    public void testNoArrayAsFilterElements() throws IOException {
        String queryString = """
            {
              "%s": {
                "filter": [ { "term": { "foo": "a" } }, { "term": { "foo": "x" } } ]
              }
            }""".formatted(ConstantScoreQueryBuilder.NAME);
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryString));
        assertThat(e.getMessage(), containsString("unexpected token [START_ARRAY]"));
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new ConstantScoreQueryBuilder((QueryBuilder) null));
    }

    @Override
    public void testUnknownField() {
        assumeTrue("test doesn't apply for query filter queries", false);
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "constant_score" : {
                "filter" : {
                  "terms" : {
                    "user" : [ "kimchy", "elasticsearch" ],
                    "boost" : 42.0
                  }
                },
                "boost" : 23.0
              }
            }""";

        ConstantScoreQueryBuilder parsed = (ConstantScoreQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 23.0, parsed.boost(), 0.0001);
        assertEquals(json, 42.0, parsed.innerQuery().boost(), 0.0001);
    }

    public void testRewriteToMatchNone() throws IOException {
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(new MatchNoneQueryBuilder());
        QueryBuilder rewrite = constantScoreQueryBuilder.rewrite(createSearchExecutionContext());
        assertEquals(rewrite, new MatchNoneQueryBuilder());
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        ConstantScoreQueryBuilder queryBuilder = new ConstantScoreQueryBuilder(new TermQueryBuilder("unmapped_field", "foo"));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testExceedMaxNestedDepth() throws IOException {
        ConstantScoreQueryBuilder query = new ConstantScoreQueryBuilder(new ConstantScoreQueryBuilder(
            new ConstantScoreQueryBuilder(new ConstantScoreQueryBuilder(RandomQueryBuilder.createQuery(random())))));
        AbstractQueryBuilder.setMaxNestedDepth(2);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, query.toString())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseTopLevelQuery(parser));
            assertEquals(
                "The nested depth of the query exceeds the maximum nested depth for queries set in ["
                    + INDICES_MAX_NESTED_DEPTH_SETTING.getKey()
                    + "]",
                e.getMessage()
            );
        } finally {
            AbstractQueryBuilder.setMaxNestedDepth(INDICES_MAX_NESTED_DEPTH_SETTING.getDefault(Settings.EMPTY));
        }
    }
}
