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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;

public final class SourceOnlyBinaryDocValues extends BinaryDocValues {
    private final LeafReaderContext leafReaderContext;
    private final String field;
    private final SourceLookup sourceLookup;
    private final DocIdSetIterator docIdSetIterator;

    SourceOnlyBinaryDocValues(LeafReaderContext leafReaderContext, String field, SourceLookup sourceLookup) {
        this.docIdSetIterator = DocIdSetIterator.all(leafReaderContext.reader().maxDoc());
        this.leafReaderContext = leafReaderContext;
        this.field = field;
        this.sourceLookup = sourceLookup;
    }

    @Override
    public BytesRef binaryValue() {
        sourceLookup.setSegmentAndDocument(leafReaderContext, docIdSetIterator.docID());
        //TODO calling toString here may not be the best thing to do,
        // or we should at least check what we got back from _source?
        // once field-retrieval is implemented we can reuse the same method added to field mapper
        // which will do the right type conversion?
        Object obj = sourceLookup.extractValue(field);
        return obj == null ? null : new BytesRef(obj.toString());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        advance(target);
        //TODO keeping it simple for now, but this will make us extract the same value twice from _source.
        return binaryValue() != null;
    }

    @Override
    public int docID() {
        return docIdSetIterator.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return docIdSetIterator.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return docIdSetIterator.advance(target);
    }

    @Override
    public long cost() {
        return docIdSetIterator.cost();
    }

    SortedBinaryDocValues toSortedBinaryDocValues() {
        return new SortedBinaryDocValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return SourceOnlyBinaryDocValues.this.advanceExact(doc);
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public BytesRef nextValue() {
                return binaryValue();
            }
        };
    }
}
