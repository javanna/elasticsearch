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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.lookup.SourceLookup;

public class SourceOnlyBinaryFieldData extends RuntimeBinaryFieldData {

    public static class Builder implements IndexFieldData.Builder {
        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType,
                                       IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                       MapperService mapperService) {
            return new SourceOnlyBinaryFieldData(indexSettings, fieldType.name(), cache);
        }
    }

    private SourceOnlyBinaryFieldData(IndexSettings indexSettings, String fieldName, IndexFieldDataCache cache) {
        super(indexSettings, fieldName, cache);
    }

    private final SetOnce<SourceLookup> sourceLookup = new SetOnce<>();

    //TODO write test for source being loaded once per document in the context of the same search request
    public void setSourceLookup(SourceLookup sourceLookup) {
        this.sourceLookup.set(sourceLookup);
    }

    @Override
    protected BytesRef getValue(LeafReaderContext leafReaderContext, int docID) {
        return RuntimeValueProducer.loadBinaryFromSource(getFieldName(), sourceLookup.get())
            .produce(leafReaderContext, docID);
    }
}
