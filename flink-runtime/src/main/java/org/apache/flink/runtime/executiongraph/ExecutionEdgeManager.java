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
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Class that manages all data connections between tasks.
 */
public class ExecutionEdgeManager {

	private final ConcurrentMap<IntermediateResultPartitionID, List<ExecutionVertex>> partitionConsumers = new ConcurrentHashMap<>();

	private final ConcurrentMap<ExecutionVertexID, List<IntermediateResultPartition[]>> vertexConsumedPartitions = new ConcurrentHashMap<>();

	public void setVertexConsumedPartitions(
		ExecutionVertexID executionVertexId,
		IntermediateResultPartition[] partitions,
		int inputNumber) {

		final List<IntermediateResultPartition[]> consumedPartitions = getVertexAllConsumedPartitions(
			executionVertexId);

		// sanity check
		checkState(consumedPartitions.size() == inputNumber);

		consumedPartitions.add(partitions);
	}

	public List<IntermediateResultPartition[]> getVertexAllConsumedPartitions(ExecutionVertexID executionVertexId) {
		return vertexConsumedPartitions.computeIfAbsent(executionVertexId, id -> new ArrayList<>());
	}

	public void setPartitionConsumer(
		IntermediateResultPartitionID resultPartitionId,
		ExecutionVertex consumerVertex) {

		final List<ExecutionVertex> consumerVertices = getPartitionConsumers(resultPartitionId);

		consumerVertices.add(consumerVertex);
	}

	public List<ExecutionVertex> getPartitionConsumers(IntermediateResultPartitionID resultPartitionId) {
		return partitionConsumers.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
	}

}
