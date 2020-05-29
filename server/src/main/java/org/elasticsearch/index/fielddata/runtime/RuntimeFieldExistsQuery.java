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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

import java.util.Objects;

public final class RuntimeFieldExistsQuery extends Query {

    private final String field;
    private final RuntimeValueProducer<BytesRef> valueProducer;

    public RuntimeFieldExistsQuery(String field, RuntimeValueProducer<BytesRef> valueProducer) {
        this.field = Objects.requireNonNull(field);
        this.valueProducer = valueProducer;
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
            field.equals(((RuntimeFieldExistsQuery) other).field);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + field.hashCode();
    }

    @Override
    public String toString(String field) {
        return "RuntimeFieldExistsQuery [field=" + this.field + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) {
                RuntimeBinaryDocValues docValues = new RuntimeBinaryDocValues(
                    context.reader().maxDoc(), docID -> valueProducer.produce(context, docID));
                TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(docValues) {
                    @Override
                    public boolean matches() {
                        BytesRef value = docValues.binaryValue();
                        return value != null;
                    }

                    @Override
                    public float matchCost() {
                        return docValues.cost();
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhaseIterator);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }

        };
    }
}
