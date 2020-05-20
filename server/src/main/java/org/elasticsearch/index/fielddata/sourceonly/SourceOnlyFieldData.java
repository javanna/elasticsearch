/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.sourceonly;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.AbstractIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

//TODO add test for source being loaded once per document in the context of the same search request
public class SourceOnlyFieldData extends AbstractIndexFieldData<SourceOnlyFieldData.SourceOnlyLeafFieldData> {
    public static class Builder implements IndexFieldData.Builder {
        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType,
                                       IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                       MapperService mapperService) {
            return new SourceOnlyFieldData(indexSettings, fieldType.name(), cache);
        }
    }

    private final SetOnce<SourceLookup> sourceLookup = new SetOnce<>();

    private SourceOnlyFieldData(IndexSettings indexSettings, String fieldName, IndexFieldDataCache cache) {
        super(indexSettings, fieldName, cache);
    }

    public void setSourceLookup(SourceLookup sourceLookup) {
        this.sourceLookup.set(sourceLookup);
    }

    @Override
    protected SourceOnlyLeafFieldData empty(int maxDoc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SourceOnlyLeafFieldData load(LeafReaderContext context) {
        return loadDirect(context);
    }

    @Override
    public SourceOnlyLeafFieldData loadDirect(LeafReaderContext context) {
        return new SourceOnlyLeafFieldData(context, getFieldName(), sourceLookup.get());
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode,
                               XFieldComparatorSource.Nested nested, boolean reverse) {
        final XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue,
            sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode,
                                        XFieldComparatorSource.Nested nested, SortOrder sortOrder,
                                        DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {

        throw new IllegalArgumentException("only supported on numeric fields");
    }

    protected static class SourceOnlyLeafFieldData implements LeafFieldData {
        private final LeafReaderContext leafReaderContext;
        private final String field;
        private final SourceLookup sourceLookup;

        private SourceOnlyLeafFieldData(LeafReaderContext leafReaderContext, String field, SourceLookup sourceLookup) {
            this.leafReaderContext = leafReaderContext;
            this.field = field;
            this.sourceLookup = sourceLookup;
        }

        @Override
        public ScriptDocValues<?> getScriptValues() {
            return new ScriptDocValues.BytesRefs(getBytesValues());
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return new SourceOnlyBinaryDocValues(leafReaderContext, field, sourceLookup).toSortedBinaryDocValues();
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {
        }
    }
}
