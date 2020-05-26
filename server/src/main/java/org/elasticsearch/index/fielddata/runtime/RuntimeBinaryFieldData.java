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

package org.elasticsearch.index.fielddata.runtime;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.AbstractIndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.util.function.Function;

public abstract class RuntimeBinaryFieldData
    extends AbstractIndexFieldData<RuntimeBinaryFieldData.RuntimeLeafFieldData> {

    protected RuntimeBinaryFieldData(IndexSettings indexSettings, String fieldName, IndexFieldDataCache cache) {
        super(indexSettings, fieldName, cache);
    }

    @Override
    protected RuntimeLeafFieldData empty(int maxDoc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RuntimeLeafFieldData load(LeafReaderContext context) {
        return loadDirect(context);
    }

    @Override
    public RuntimeLeafFieldData loadDirect(LeafReaderContext context) {
        return new RuntimeLeafFieldData(context.reader().maxDoc(), docID -> getValue(context, docID));
    }

    protected abstract BytesRef getValue(LeafReaderContext leafReaderContext, int docID);

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

    protected static class RuntimeLeafFieldData implements LeafFieldData {
        private final SortedBinaryDocValues docValues;

        private RuntimeLeafFieldData(int maxDoc, Function<Integer, BytesRef> valueExtractor) {
            this.docValues = new RuntimeBinaryDocValues(maxDoc, valueExtractor).toSortedBinaryDocValues();
        }

        @Override
        public ScriptDocValues<?> getScriptValues() {
            return new ScriptDocValues.BytesRefs(getBytesValues());
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return docValues;
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
