/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.lucene.search.postingshighlight;

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.internal.AnalyzerMapper;
import org.elasticsearch.search.highlight.HighlightUtils;
import org.elasticsearch.search.highlight.HighlighterContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Subclass of the {@link XPostingsHighlighter} that works for a single field in a single document.
 * It receives the field values as input and it performs discrete highlighting on each single value
 * calling the highlightDoc method multiple times.
 * It allows to pass in the query terms to avoid calling extract terms multiple times.
 *
 * The use that we make of the postings highlighter is not optimal. It would be much better to
 * highlight multiple docs in a single call, as we actually lose its sequential IO.  But that would require:
 * 1) to make our fork more complex and harder to maintain to perform discrete highlighting (needed to return
 * a different snippet per value when number_of_fragments=0 and the field has multiple values)
 * 2) refactoring of the elasticsearch highlight api which currently works per hit
 *
 */
public final class CustomPostingsHighlighter extends XPostingsHighlighter {

    private static final Passage[] EMPTY_PASSAGE = new Passage[0];

    private final CustomPassageFormatter passageFormatter;
    private final HighlighterContext highlighterContext;

    private BreakIterator breakIterator;

    public CustomPostingsHighlighter(CustomPassageFormatter passageFormatter, HighlighterContext highlighterContext, int maxLength) {
        super(maxLength);
        this.passageFormatter = passageFormatter;
        this.highlighterContext = highlighterContext;
    }

    public void setBreakIterator(BreakIterator breakIterator) {
        this.breakIterator = breakIterator;
    }

    @Override
    protected Analyzer getIndexAnalyzer(String field) {
        AnalyzerMapper analyzerMapper = highlighterContext.context.mapperService().documentMapper(highlighterContext.hitContext.hit().type()).analyzerMapper();
        return analyzerMapper.setAnalyzer(highlighterContext);
    }

    @Override
    protected PassageFormatter getFormatter(String field) {
        return passageFormatter;
    }

    @Override
    protected BreakIterator getBreakIterator(String field) {
        if (breakIterator == null) {
            return super.getBreakIterator(field);
        }
        return breakIterator;
    }

    @Override
    protected char getMultiValuedSeparator(String field) {
        //U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting
        return HighlightUtils.PARAGRAPH_SEPARATOR;
    }

    /*
    By default the postings highlighter returns non highlighted snippet when there are no matches.
    We want to return no snippets by default, unless no_match_size is greater than 0
     */
    @Override
    protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
        if (highlighterContext.field.fieldOptions().noMatchSize() > 0) {
            //we want to return the first sentence of the first snippet only
            return super.getEmptyHighlight(fieldName, bi, 1);
        }
        return EMPTY_PASSAGE;
    }

    @Override
    protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
        SearchContext searchContext = SearchContext.current();
        boolean forceSource = searchContext.highlight().forceSource(highlighterContext.field);
        if (forceSource || !highlighterContext.mapper.fieldType().stored()) {
            List<Object> objects = HighlightUtils.loadFromSource(highlighterContext.mapper, searchContext, highlighterContext.hitContext);
            StringBuilder value = new StringBuilder();
            for (Object object : objects) {
                value.append(object).append(getMultiValuedSeparator(highlighterContext.field.field()));
            }
            return new String[][]{{value.toString()}};
        }
        return super.loadFieldValues(searcher, fields, docids, maxLength);
    }

    public Map<String,Map<Integer, List<Snippet>>> highlightSnippets(String fieldsIn[], Query query, IndexSearcher searcher, int[] docidsIn, int maxPassagesIn[]) throws IOException {
        Map<String,Map<Integer, List<Snippet>>> snippets = new HashMap<>();
        for(Map.Entry<String,Object[]> ent : highlightFieldsAsObjects(fieldsIn, query, searcher, docidsIn, maxPassagesIn).entrySet()) {
            Map<Integer, List<Snippet>> docsSnippets = new HashMap<>();
            Object[] snippetObjects = ent.getValue();
            for (int i = 0; i < snippetObjects.length; i++) {
                Object docSnippetObject = snippetObjects[i];
                List<Snippet> docSnippets = Lists.newArrayList();
                if (docSnippetObject != null) {
                    //we return multiple snippets instead of merging back the passages into a single string
                    for (Snippet snippet : (Snippet[]) docSnippetObject) {
                        if (snippet != null) {
                            docSnippets.add(snippet);
                        }
                    }
                }
                docsSnippets.put(docidsIn[i], docSnippets);
            }
            snippets.put(ent.getKey(), docsSnippets);
        }
        return snippets;
    }
}
