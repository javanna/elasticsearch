/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.highlight;

import com.google.common.collect.Maps;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.postingshighlight.CustomPassageFormatter;
import org.apache.lucene.search.postingshighlight.CustomPostingsHighlighter;
import org.apache.lucene.search.postingshighlight.Snippet;
import org.apache.lucene.search.postingshighlight.WholeBreakIterator;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.*;

public class PostingsHighlighter implements Highlighter {

    private static final String CACHE_KEY = "highlight-postings";

    @Override
    public String[] names() {
        return new String[]{"postings", "postings-highlighter"};
    }

    @Override
    public HighlightField highlight(HighlighterContext highlighterContext) {

        FieldMapper<?> fieldMapper = highlighterContext.mapper;
        SearchContextHighlight.Field field = highlighterContext.field;
        if (fieldMapper.fieldType().indexOptions() != FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
            throw new ElasticsearchIllegalArgumentException("the field [" + highlighterContext.fieldName + "] should be indexed with positions and offsets in the postings list to be used with postings highlighter");
        }

        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;

        //TODO terms are not cached, extract terms will be called once per document, per field since we don't do bulk highlighting yet
        // a temporary solution might also be to allow to provide or manipulate the terms ourselves, so that we can cache them
        if (!hitContext.cache().containsKey(CACHE_KEY)) {
            hitContext.cache().put(CACHE_KEY, new HighlighterEntry());
        }

        HighlighterEntry highlighterEntry = (HighlighterEntry) hitContext.cache().get(CACHE_KEY);
        MapperHighlighterEntry mapperHighlighterEntry = highlighterEntry.mappers.get(fieldMapper);

        if (mapperHighlighterEntry == null) {
            Encoder encoder = field.fieldOptions().encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;
            CustomPassageFormatter passageFormatter = new CustomPassageFormatter(field.fieldOptions().preTags()[0], field.fieldOptions().postTags()[0], encoder);
            mapperHighlighterEntry = new MapperHighlighterEntry(passageFormatter);
        }


        List<Snippet> fieldSnippets;
        int numberOfFragments;

        try {
            CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(mapperHighlighterEntry.passageFormatter, highlighterContext, Integer.MAX_VALUE-1);

            if (field.fieldOptions().numberOfFragments() == 0) {
                highlighter.setBreakIterator(new WholeBreakIterator());
                numberOfFragments = 1; //1 per value since we highlight per value
            } else {
                numberOfFragments = field.fieldOptions().numberOfFragments();
            }

            Map<String, Map<Integer, List<Snippet>>> highlightSnippets = highlighter.highlightSnippets(new String[]{fieldMapper.names().indexName()}, highlighterContext.query.originalQuery(), context.searcher(), new int[]{hitContext.topLevelDocId()}, new int[]{numberOfFragments});
            assert highlightSnippets.size() == 1;
            Map<Integer, List<Snippet>> docsSnippets = highlightSnippets.get(fieldMapper.names().indexName());
            assert docsSnippets != null;
            assert docsSnippets.size() == 1;
            fieldSnippets = docsSnippets.get(hitContext.topLevelDocId());
            assert fieldSnippets != null;

        } catch(IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
        }

        List<Snippet> snippets = filterSnippets(fieldSnippets, field.fieldOptions().numberOfFragments());

        if (field.fieldOptions().scoreOrdered()) {
            //let's sort the snippets by score if needed
            CollectionUtil.introSort(snippets, new Comparator<Snippet>() {
                public int compare(Snippet o1, Snippet o2) {
                    return (int) Math.signum(o2.getScore() - o1.getScore());
                }
            });
        }

        String[] fragments = new String[snippets.size()];
        for (int i = 0; i < fragments.length; i++) {
            fragments[i] = snippets.get(i).getText();
        }

        if (fragments.length > 0) {
            return new HighlightField(highlighterContext.fieldName, StringText.convertFromStringArray(fragments));
        }

        return null;
    }

    private static List<Snippet> filterSnippets(List<Snippet> snippets, int numberOfFragments) {

        //We need to filter the snippets as due to no_match_size we could have
        //either highlighted snippets together non highlighted ones
        //We don't want to mix those up
        List<Snippet> filteredSnippets = new ArrayList<>(snippets.size());
        for (Snippet snippet : snippets) {
            if (snippet != null && snippet.isHighlighted()) {
                filteredSnippets.add(snippet);
            }
        }

        //if there's at least one highlighted snippet, we return all the highlighted ones
        //otherwise we return the first non highlighted one if available
        if (filteredSnippets.size() == 0) {
            if (snippets.size() > 0) {
                Snippet snippet = snippets.get(0);
                //if we did discrete per value highlighting using whole break iterator (as number_of_fragments was 0)
                //we need to obtain the first sentence of the first value
                if (numberOfFragments == 0) {
                    BreakIterator bi = BreakIterator.getSentenceInstance(Locale.ROOT);
                    String text = snippet.getText();
                    bi.setText(text);
                    int next = bi.next();
                    if (next != BreakIterator.DONE) {
                        String newText = text.substring(0, next).trim();
                        snippet = new Snippet(newText, snippet.getScore(), snippet.isHighlighted());
                    }
                }
                filteredSnippets.add(snippet);
            }
        }

        return filteredSnippets;
    }

    private static class HighlighterEntry {
        Map<FieldMapper<?>, MapperHighlighterEntry> mappers = Maps.newHashMap();
    }

    private static class MapperHighlighterEntry {
        final CustomPassageFormatter passageFormatter;

        private MapperHighlighterEntry(CustomPassageFormatter passageFormatter) {
            this.passageFormatter = passageFormatter;
        }
    }
}
