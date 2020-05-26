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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.function.Function;

public final class RuntimeBinaryDocValues extends BinaryDocValues {
    private final Function<Integer, BytesRef> valueExtractor;
    private final DocIdSetIterator docIdSetIterator;

    RuntimeBinaryDocValues(int maxDoc, Function<Integer, BytesRef> valueExtractor) {
        this.docIdSetIterator = DocIdSetIterator.all(maxDoc);
        this.valueExtractor = valueExtractor;
    }

    @Override
    public BytesRef binaryValue() {
        return this.valueExtractor.apply(docIdSetIterator.docID());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        advance(target);
        //TODO keeping it simple for now, but this will make us extract the same value twice from _source:
        // 1) first time to return a boolean in this method 2) when reading the actual value. Though this will not cause _source to be
        // loaded and parsed twice so it should be ok, at least for now.
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
                return RuntimeBinaryDocValues.this.advanceExact(doc);
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
