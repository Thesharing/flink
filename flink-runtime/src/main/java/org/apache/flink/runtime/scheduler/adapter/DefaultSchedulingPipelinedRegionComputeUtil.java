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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.topology.Group;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.mergeRegions;
import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.mergeRegionsOnCycles;

/**
 * Utility for computing {@link SchedulingPipelinedRegion}.
 */
public final class DefaultSchedulingPipelinedRegionComputeUtil {

	public static Set<Set<SchedulingExecutionVertex>> computePipelinedRegions(
		final Iterable<DefaultExecutionVertex> topologicallySortedVertexes) {
		final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion = buildRawRegions(
			topologicallySortedVertexes);
		return mergeRegionsOnCycles(vertexToRegion);
	}

	private static Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> buildRawRegions(
		final Iterable<? extends DefaultExecutionVertex> topologicallySortedVertexes) {

		final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion = new IdentityHashMap<>();

		// iterate all the vertices which are topologically sorted
		for (DefaultExecutionVertex vertex : topologicallySortedVertexes) {
			Set<SchedulingExecutionVertex> currentRegion = new HashSet<>();
			currentRegion.add(vertex);
			vertexToRegion.put(vertex, currentRegion);

			for (Group<IntermediateResultPartitionID> consumedResultIds : vertex.getGroupedConsumedResults()) { // TODO: WORRIED!
				// Similar to the BLOCKING ResultPartitionType, each vertex connected through PIPELINED_APPROXIMATE
				// is also considered as a single region. This attribute is called "reconnectable".
				// reconnectable will be removed after FLINK-19895, see also {@link ResultPartitionType#isReconnectable}
				for (IntermediateResultPartitionID consumerId : consumedResultIds.getItems()) {
					DefaultResultPartition consumedResult = vertex.getResultPartition(consumerId);
					if (!consumedResult.getResultType().isReconnectable()) {
						final DefaultExecutionVertex producerVertex = consumedResult.getProducer();
						final Set<SchedulingExecutionVertex> producerRegion = vertexToRegion.get(
							producerVertex);

						if (producerRegion == null) {
							throw new IllegalStateException(
								"Producer task " + producerVertex.getId()
									+ " failover region is null while calculating failover region for the consumer task "
									+ vertex.getId()
									+ ". This should be a failover region building bug.");
						}

						// check if it is the same as the producer region, if so skip the merge
						// this check can significantly reduce compute complexity in All-to-All PIPELINED edge case
						if (currentRegion != producerRegion) {
							currentRegion = mergeRegions(
								currentRegion,
								producerRegion,
								vertexToRegion);
						}
					} else {
						break;
					}
				}
			}
		}
		return vertexToRegion;
	}
}
