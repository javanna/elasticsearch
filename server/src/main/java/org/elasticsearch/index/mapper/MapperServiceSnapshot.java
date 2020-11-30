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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public final class MapperServiceSnapshot {
    private final DocumentMapper mapper;

    public MapperServiceSnapshot(DocumentMapper documentMapper) {
        this.mapper = documentMapper;
    }

    public MappedFieldType fieldType(String fullName) {
        if (fullName.equals(TypeFieldType.NAME)) {
            return new TypeFieldType(this.mapper == null ? "_doc" : this.mapper.type());
        }
        return this.mapper == null ? null : this.mapper.mappers().fieldTypes().get(fullName);
    }

    public ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException {
        return mapper == null ? null : mapper.parse(source);
    }

    public boolean hasNested() {
        return this.mapper != null && this.mapper.hasNestedObjects();
    }

    public boolean hasMappings() {
        return mapper != null;
    }

    public Set<String> simpleMatchToFullName(String pattern) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            return Collections.singleton(pattern);
        }
        return this.mapper == null ? Collections.emptySet() : this.mapper.mappers().fieldTypes().simpleMatchToFullName(pattern);
    }

    public ObjectMapper getObjectMapper(String name) {
        return this.mapper == null ? null : this.mapper.mappers().objectMappers().get(name);
    }

    public Set<String> sourcePath(String fullName) {
        return this.mapper == null ? Collections.emptySet() : this.mapper.mappers().fieldTypes().sourcePaths(fullName);
    }

    public boolean isSourceEnabled() {
        return mapper.sourceMapper().enabled();
    }

    public NamedAnalyzer indexAnalyzer(String field, Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
        if (this.mapper == null) {
            return unindexedFieldAnalyzer.apply(field);
        }
        return this.mapper.mappers().indexAnalyzer(field, unindexedFieldAnalyzer);
    }

    public boolean containsBrokenAnalysis(String field) {
        NamedAnalyzer a = indexAnalyzer(field, f -> null);
        if (a == null) {
            return false;
        }
        return a.containsBrokenAnalysis();
    }

}
