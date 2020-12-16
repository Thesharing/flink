package org.apache.flink.test.runtime.performance;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.createDefaultJobVertices;
import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.createExecutionGraph;
import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.createJobGraph;

/**
 * Performance test for topology building.
 */
public class TopologyBuildingPerformanceTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(TopologyBuildingPerformanceTest.class);

	public static final int PARALLELISM = 8000;

	@Test
	public void testBuildExecutionGraphInStreamingJobPerformance() throws Exception {
		final List<JobVertex> jobVertices = createDefaultJobVertices(
			PARALLELISM,
			DistributionPattern.ALL_TO_ALL,
			ResultPartitionType.PIPELINED);
		final JobGraph jobGraph = createJobGraph(
			jobVertices,
			ScheduleMode.EAGER,
			ExecutionMode.PIPELINED);
		final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		final long startTime = System.nanoTime();

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		final long duration = (System.nanoTime() - startTime) / 1_000_000;

		LOG.info(String.format("Duration of building execution graph is : %d ms", duration));
	}

	@Test
	public void testBuildExecutionGraphInBatchJobPerformance() throws Exception {
		final List<JobVertex> jobVertices = createDefaultJobVertices(
			PARALLELISM,
			DistributionPattern.ALL_TO_ALL,
			ResultPartitionType.BLOCKING);
		final JobGraph jobGraph = createJobGraph(
			jobVertices,
			ScheduleMode.LAZY_FROM_SOURCES,
			ExecutionMode.BATCH);
		final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		final long startTime = System.nanoTime();

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		final long duration = (System.nanoTime() - startTime) / 1_000_000;

		LOG.info(String.format("Duration of building execution graph is : %d ms", duration));
	}
}
