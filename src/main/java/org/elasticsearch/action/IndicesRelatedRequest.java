/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action;

import com.google.common.collect.Sets;
import org.elasticsearch.cluster.metadata.MetaData;

import java.util.Set;

/**
 * Needs to be implemented by all {@link org.elasticsearch.action.ActionRequest} subclasses that relate to
 * one or more indices. Allows to retrieve which indices the action relates to.
 */
public interface IndicesRelatedRequest {

    /**
     * Returns a set of unique indices the action relates to
     */
    Set<String> requestedIndices();

    public static class Helper {
        public static Set<String> indicesOrAll(String... indices) {
            if (MetaData.isAllIndices(indices)) {
                return Sets.newHashSet(MetaData.ALL);
            }
            return Sets.newHashSet(indices);
        }
    }
}
