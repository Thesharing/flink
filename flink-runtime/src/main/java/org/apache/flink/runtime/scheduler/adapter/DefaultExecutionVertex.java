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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.topology.Group;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link SchedulingExecutionVertex}.
 */
class DefaultExecutionVertex implements SchedulingExecutionVertex {

	private final ExecutionVertexID executionVertexId;

	private final List<DefaultResultPartition> producedResults;

	private final Supplier<ExecutionState> stateSupplier;

	private final InputDependencyConstraint inputDependencyConstraint;

	private final List<Group<IntermediateResultPartitionID>> consumedPartitionIds;

	private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionById;

	DefaultExecutionVertex(
			ExecutionVertexID executionVertexId,
			List<DefaultResultPartition> producedPartitions,
			Supplier<ExecutionState> stateSupplier,
			InputDependencyConstraint constraint,
			List<Group<IntermediateResultPartitionID>> consumedPartitionIds,
			Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionById) {
		this.executionVertexId = checkNotNull(executionVertexId);
		this.stateSupplier = checkNotNull(stateSupplier);
		this.producedResults = checkNotNull(producedPartitions);
		this.inputDependencyConstraint = checkNotNull(constraint);
		this.consumedPartitionIds = consumedPartitionIds;
		this.resultPartitionById = resultPartitionById;
	}

	@Override
	public ExecutionVertexID getId() {
		return executionVertexId;
	}

	@Override
	public ExecutionState getState() {
		return stateSupplier.get();
	}

	@Override
	public Iterable<DefaultResultPartition> getConsumedResults() {
		/*return getGroupedConsumedResults()
			.stream()
			.map(Group::getItems)
			.flatMap(Collection::stream)
			.map(topology::getResultPartition)
			.collect(Collectors.toList());*/
		final List<Group<IntermediateResultPartitionID>> consumers = getGroupedConsumedResults();

		return () -> new Iterator<DefaultResultPartition>() {
			private int groupIdx = 0;
			private int idx = 0;

			@Override
			public boolean hasNext() {
				if (groupIdx < consumers.size() &&
					idx >= consumers.get(groupIdx).getItems().size()) {
					++groupIdx;
					idx = 0;
				}
				return groupIdx < consumers.size() &&
					idx < consumers.get(groupIdx).getItems().size();
			}

			@Override
			public DefaultResultPartition next() {
				if (hasNext()) {
					return getResultPartition(
						consumers.get(groupIdx).getItems().get(idx++));
				} else {
					throw new NoSuchElementException();
				}
			}
		};
	}

	@Override
	public List<Group<IntermediateResultPartitionID>> getGroupedConsumedResults() {
		return consumedPartitionIds;
	}

	public DefaultResultPartition getResultPartition(IntermediateResultPartitionID id) {
		return resultPartitionById.get(id);
	}

	@Override
	public Iterable<DefaultResultPartition> getProducedResults() {
		return producedResults;
	}

	@Override
	public InputDependencyConstraint getInputDependencyConstraint() {
		return inputDependencyConstraint;
	}
}
