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

package org.elasticsearch.action.search;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.routing.ShardIterator;

import java.util.Iterator;
import java.util.List;

//TODO we may want to get rid of this class
public class SearchShardsIterator implements Iterable<SearchShardIterator> {

    private final List<SearchShardIterator> iterators;

    /**
     * Constructs a enw GroupShardsIterator from the given list.
     */
    public SearchShardsIterator(List<SearchShardIterator> iterators) {
        CollectionUtil.timSort(iterators);
        this.iterators = iterators;
    }

    /**
     * Returns the total number of shards plus the number of empty groups
     * @return number of shards and empty groups
     */
    public int totalSizeWith1ForEmpty() {
        int size = 0;
        for (ShardIterator shard : iterators) {
            size += Math.max(1, shard.size());
        }
        return size;
    }

    /**
     * Return the number of groups
     * @return number of groups
     */
    public int size() {
        return iterators.size();
    }

    @Override
    public Iterator<SearchShardIterator> iterator() {
        return iterators.iterator();
    }

}
