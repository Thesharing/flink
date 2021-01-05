/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.topology.Group;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Class that manages all the connections between tasks. */
public class EdgeManager {

    private final Map<IntermediateResultPartitionID, List<Group<ExecutionVertexID>>>
            partitionConsumers = new HashMap<>();

    private final Map<ExecutionVertexID, List<Group<IntermediateResultPartitionID>>>
            vertexConsumedPartitions = new HashMap<>();

    public void setVertexConsumedPartitions(
            ExecutionVertexID executionVertexId,
            Group<IntermediateResultPartitionID> partitions,
            int inputNumber) {

        final List<Group<IntermediateResultPartitionID>> consumedPartitions =
                getVertexConsumedPartitions(executionVertexId);

        // sanity check
        checkState(consumedPartitions.size() == inputNumber);

        consumedPartitions.add(partitions);
    }

    public List<Group<IntermediateResultPartitionID>> getVertexConsumedPartitions(
            ExecutionVertexID executionVertexId) {
        return vertexConsumedPartitions.computeIfAbsent(executionVertexId, id -> new ArrayList<>());
    }

    public void setPartitionConsumers(
            IntermediateResultPartitionID resultPartitionId,
            Group<ExecutionVertexID> consumerVertices) {

        checkState(!partitionConsumers.containsKey(resultPartitionId));

        final List<Group<ExecutionVertexID>> consumers = getPartitionConsumers(resultPartitionId);

        // sanity check
        checkState(
                consumers.size() == 0,
                "Currently there has to be exactly one consumer in real jobs");

        consumers.add(consumerVertices);
    }

    public List<Group<ExecutionVertexID>> getPartitionConsumers(
            IntermediateResultPartitionID resultPartitionId) {
        return partitionConsumers.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    public Map<IntermediateResultPartitionID, List<Group<ExecutionVertexID>>>
            getAllPartitionConsumers() {
        return partitionConsumers;
    }

    public Map<ExecutionVertexID, List<Group<IntermediateResultPartitionID>>>
            getAllVertexConsumedPartitions() {
        return vertexConsumedPartitions;
    }
}
