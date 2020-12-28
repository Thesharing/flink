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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.topology.Group;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link SchedulingResultPartition}.
 */
class DefaultResultPartition implements SchedulingResultPartition {

	private final IntermediateResultPartitionID resultPartitionId;

	private final IntermediateDataSetID intermediateDataSetId;

	private final ResultPartitionType partitionType;

	private final Supplier<ResultPartitionState> resultPartitionStateSupplier;

	private DefaultExecutionVertex producer;

	private final List<Group<ExecutionVertexID>> consumerIds;

	private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVertexById;

	DefaultResultPartition(
		IntermediateResultPartitionID partitionId,
		IntermediateDataSetID intermediateDataSetId,
		ResultPartitionType partitionType,
		Supplier<ResultPartitionState> resultPartitionStateSupplier,
		List<Group<ExecutionVertexID>> consumerIds,
		Map<ExecutionVertexID, DefaultExecutionVertex> executionVertexById) {

		this.resultPartitionId = checkNotNull(partitionId);
		this.intermediateDataSetId = checkNotNull(intermediateDataSetId);
		this.partitionType = checkNotNull(partitionType);
		this.resultPartitionStateSupplier = checkNotNull(resultPartitionStateSupplier);
		this.consumerIds = consumerIds;
		this.executionVertexById = executionVertexById;
	}

	@Override
	public IntermediateResultPartitionID getId() {
		return resultPartitionId;
	}

	@Override
	public IntermediateDataSetID getResultId() {
		return intermediateDataSetId;
	}

	@Override
	public ResultPartitionType getResultType() {
		return partitionType;
	}

	@Override
	public ResultPartitionState getState() {
		return resultPartitionStateSupplier.get();
	}

	@Override
	public DefaultExecutionVertex getProducer() {
		return producer;
	}

	@Override
	public Iterable<DefaultExecutionVertex> getConsumers() {
		/* return getGroupedConsumers()
			.stream()
			.map(Group::getItems)
			.flatMap(Collection::stream)
			.map(topology::getVertex)
			.collect(Collectors.toList()); */
		final List<Group<ExecutionVertexID>> consumers = getGroupedConsumers();

		return () -> new Iterator<DefaultExecutionVertex>() {
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
			public DefaultExecutionVertex next() {
				if (hasNext()) {
					return getVertex(consumers.get(groupIdx).getItems().get(idx++));
				} else {
					throw new NoSuchElementException();
				}
			}
		};
	}

	@Override
	public List<Group<ExecutionVertexID>> getGroupedConsumers() {
		return consumerIds;
	}

	public DefaultExecutionVertex getVertex(ExecutionVertexID id) {
		return executionVertexById.get(id);
	}

	void setProducer(DefaultExecutionVertex vertex) {
		producer = checkNotNull(vertex);
	}
}
