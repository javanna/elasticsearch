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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
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
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Objects;

public class SourceOnlyTermQuery extends Query {

    private final Term term;
    private final SourceLookup sourceLookup;

    public SourceOnlyTermQuery(Term term, SourceLookup sourceLookup) {
        this.term = term;
        this.sourceLookup = sourceLookup;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) {
                SourceOnlyBinaryDocValues docValues = new SourceOnlyBinaryDocValues(
                    context, term.field(), sourceLookup);
                TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(docValues) {
                    @Override
                    public boolean matches() {
                        BytesRef value = docValues.binaryValue();
                        return Objects.equals(value, term.bytes());
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
                return false;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(term.field())) {
            visitor.consumeTerms(this, term);
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(term.text());
        return buffer.toString();
    }


    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
            term.equals(((SourceOnlyTermQuery) other).term);
    }

    @Override
    public int hashCode() {
        return classHash() ^ term.hashCode();
    }}
