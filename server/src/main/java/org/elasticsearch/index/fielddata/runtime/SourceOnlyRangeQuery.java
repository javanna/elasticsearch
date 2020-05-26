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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;

//TODO is it ok that we don't extend MultiTermQuery?
public final class SourceOnlyRangeQuery extends Query {

    private final SourceLookup sourceLookup;
    private final String fieldName;
    private final BytesRef lowerTerm;
    private final BytesRef upperTerm;
    private final boolean includeLower;
    private final boolean includeUpper;

    public SourceOnlyRangeQuery(SourceLookup sourceLookup, String fieldName, BytesRef lowerTerm, BytesRef upperTerm,
                                boolean includeLower, boolean includeUpper) {
        this.sourceLookup = sourceLookup;
        this.fieldName = fieldName;
        this.lowerTerm = lowerTerm;
        this.upperTerm = upperTerm;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return super.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (fieldName.equals(field) == false) {
            buffer.append(fieldName);
            buffer.append(":");
        }
        buffer.append(includeLower ? '[' : '{');
        // TODO: all these toStrings for queries should just output the bytes, it might not be UTF-8!
        buffer.append(lowerTerm != null ? ("*".equals(Term.toString(lowerTerm)) ? "\\*" : Term.toString(lowerTerm))  : "*");
        buffer.append(" TO ");
        buffer.append(upperTerm != null ? ("*".equals(Term.toString(upperTerm)) ? "\\*" : Term.toString(upperTerm)) : "*");
        buffer.append(includeUpper ? ']' : '}');
        return buffer.toString();
    }

    //TODO
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    /*    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (includeLower ? 1231 : 1237);
        result = prime * result + (includeUpper ? 1231 : 1237);
        result = prime * result + ((lowerTerm == null) ? 0 : lowerTerm.hashCode());
        result = prime * result + ((upperTerm == null) ? 0 : upperTerm.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        SourceOnlyRangeQuery other = (SourceOnlyRangeQuery) obj;
        if (includeLower != other.includeLower)
            return false;
        if (includeUpper != other.includeUpper)
            return false;
        if (lowerTerm == null) {
            if (other.lowerTerm != null)
                return false;
        } else if (!lowerTerm.equals(other.lowerTerm))
            return false;
        if (upperTerm == null) {
            if (other.upperTerm != null)
                return false;
        } else if (!upperTerm.equals(other.upperTerm))
            return false;
        return true;
    }*/
}
