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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.lookup.SourceLookup;

@FunctionalInterface
public interface RuntimeValueProducer<T> {
    T produce(LeafReaderContext leafReaderContext, int docID);

    static RuntimeValueProducer<BytesRef> loadBinaryFromSource(String field, SourceLookup sourceLookup) {
        return (context, docID) -> {
            sourceLookup.setSegmentAndDocument(context, docID);
            //TODO once field-retrieval is merged to master we can reuse the new method added
            // to field mapper which will do the right type conversion.
            Object obj = sourceLookup.extractValue(field);
            return obj == null ? null : new BytesRef(obj.toString());
        };
    }
}

