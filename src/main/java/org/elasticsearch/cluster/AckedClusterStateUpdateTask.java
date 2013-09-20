/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.cluster;

/**
 * An extension interface to {@link ClusterStateUpdateTask} that allows to be notified when
 * the cluster state update has been acknowledged from the nodes it has been published to.
 */
public interface AckedClusterStateUpdateTask extends TimeoutClusterStateUpdateTask {

    /**
     * Called when each node replies to a new cluster state publish event, meaning
     * that the cluster state has been successfully processed on that node
     */
    void clusterStatePublishAcked();

    /**
     * Called when each node has issues processing a new cluster state
     */
    void clusterStatePublishFailed();
}
