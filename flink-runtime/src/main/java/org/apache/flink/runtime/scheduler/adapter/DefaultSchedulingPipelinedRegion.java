/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.topology.Group;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Default implementation of {@link SchedulingPipelinedRegion}.
 */
public class DefaultSchedulingPipelinedRegion implements SchedulingPipelinedRegion {

	private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVertices;

	private List<Group<IntermediateResultPartitionID>> consumedResults;

	private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById;

	public DefaultSchedulingPipelinedRegion(
		Set<DefaultExecutionVertex> defaultExecutionVertices,
		Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById) {

		Preconditions.checkNotNull(defaultExecutionVertices);

		this.resultPartitionsById = resultPartitionsById;

		this.executionVertices = new HashMap<>();
		for (DefaultExecutionVertex executionVertex : defaultExecutionVertices) {
			this.executionVertices.put(executionVertex.getId(), executionVertex);
		}
	}

	@VisibleForTesting
	public DefaultSchedulingPipelinedRegion(Set<DefaultExecutionVertex> defaultExecutionVertices) {
		this(defaultExecutionVertices, null);
	}

	@Override
	public Iterable<DefaultExecutionVertex> getVertices() {
		return Collections.unmodifiableCollection(executionVertices.values());
	}

	@Override
	public DefaultExecutionVertex getVertex(final ExecutionVertexID vertexId) {
		final DefaultExecutionVertex executionVertex = executionVertices.get(vertexId);
		if (executionVertex == null) {
			throw new IllegalArgumentException(String.format(
				"Execution vertex %s not found in pipelined region",
				vertexId));
		}
		return executionVertex;
	}

	@Override
	public Iterable<DefaultResultPartition> getConsumedResults() {
		if (consumedResults == null) {
			initializeConsumedResults();
		}
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

	private void initializeConsumedResults() {
		final Set<Group<IntermediateResultPartitionID>> consumedResults = new HashSet<>();
		for (DefaultExecutionVertex executionVertex : executionVertices.values()) {
			for (Group<IntermediateResultPartitionID> consumedResultIds :
				executionVertex.getGroupedConsumedResults()) {
				DefaultResultPartition resultPartition = executionVertex.getResultPartition(
					consumedResultIds.getItems().get(0));
				if (!executionVertices.containsKey(resultPartition.getProducer().getId())) {
					consumedResults.add(consumedResultIds);
				}
			}
		}

		this.consumedResults = new ArrayList<>();
		this.consumedResults.addAll(consumedResults);
	}

	@Override
	public List<Group<IntermediateResultPartitionID>> getGroupedConsumedResults() {
		if (consumedResults == null) {
			initializeConsumedResults();
		}
		return Collections.unmodifiableList(consumedResults);
	}

	@Override
	public DefaultResultPartition getResultPartition(IntermediateResultPartitionID id) {
		return resultPartitionsById.get(id);
	}
}
