package org.apache.flink.test.runtime.performance;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Performance test for region failover.
 */
public class RegionFailoverPerformanceTest {
	private static final Logger LOG = LoggerFactory.getLogger(DeployingPerformanceTest.class);

	public static final int PARALLELISM = 8000;

	private TestingSchedulingTopology schedulingTopology;

	@Test
	public void testCalculateRegionToRestartInStreamingJobPerformance() {
		schedulingTopology = new TestingSchedulingTopology();
		List<TestingSchedulingExecutionVertex> source =
			schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();
		List<TestingSchedulingExecutionVertex> sink =
			schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();

		schedulingTopology
			.connectAllToAll(source, sink)
			.withResultPartitionState(ResultPartitionState.CREATED)
			.withResultPartitionType(ResultPartitionType.PIPELINED_BOUNDED)
			.finish();

		for (TestingSchedulingExecutionVertex vertex : source) {
			vertex.setState(ExecutionState.RUNNING);
		}
		for (TestingSchedulingExecutionVertex vertex : sink) {
			vertex.setState(ExecutionState.RUNNING);
		}

		RestartPipelinedRegionFailoverStrategy strategy =
			new RestartPipelinedRegionFailoverStrategy(schedulingTopology);

		final long startTime = System.nanoTime();

		Set<ExecutionVertexID> tasks =
			strategy.getTasksNeedingRestart(source.get(0).getId(), new Exception("For test."));

		final long duration = (System.nanoTime() - startTime) / 1_000_000;

		LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));

		if (tasks.size() < PARALLELISM * 2) {
			throw new RuntimeException(String.format(
				"Number of tasks to restart mismatch, expected %d, actual %d.",
				PARALLELISM * 2,
				tasks.size()));
		}
	}

	@Test
	public void testCalculateRegionToRestartInBatchJobPerformance() {
		schedulingTopology = new TestingSchedulingTopology();
		List<TestingSchedulingExecutionVertex> source =
			schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();
		List<TestingSchedulingExecutionVertex> sink =
			schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();

		schedulingTopology
			.connectAllToAll(source, sink)
			.withResultPartitionState(ResultPartitionState.CREATED)
			.withResultPartitionType(ResultPartitionType.BLOCKING)
			.finish();

		for (TestingSchedulingExecutionVertex vertex : source) {
			vertex.setState(ExecutionState.FINISHED);
		}
		for (TestingSchedulingExecutionVertex vertex : source) {
			vertex.setState(ExecutionState.RUNNING);
		}

		RestartPipelinedRegionFailoverStrategy strategy =
			new RestartPipelinedRegionFailoverStrategy(schedulingTopology);

		final long startTime = System.nanoTime();

		Set<ExecutionVertexID> tasks =
			strategy.getTasksNeedingRestart(source.get(0).getId(), new Exception("For test."));

		final long duration = (System.nanoTime() - startTime) / 1_000_000;

		LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));

		if (tasks.size() < PARALLELISM + 1) {
			throw new RuntimeException(String.format(
				"Number of tasks to restart mismatch, expected %d, actual %d.",
				PARALLELISM + 1,
				tasks.size()));
		}
	}
}
