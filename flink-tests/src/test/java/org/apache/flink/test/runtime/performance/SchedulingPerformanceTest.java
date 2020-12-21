package org.apache.flink.test.runtime.performance;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulerOperations;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Performance test for task scheduling.
 */
public class SchedulingPerformanceTest {

	private static final Logger LOG = LoggerFactory.getLogger(SchedulingPerformanceTest.class);

	private static final int PARALLELISM = 4000;

	private TestingSchedulerOperations schedulerOperations;
	private TestingSchedulingTopology schedulingTopology;

	@Test
	public void testInitPipelinedRegionSchedulingStrategyInStreamingJobPerformance() {
		schedulerOperations = new TestingSchedulerOperations();
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

		final long startTime = System.nanoTime();

		final PipelinedRegionSchedulingStrategy schedulingStrategy = new PipelinedRegionSchedulingStrategy(
			schedulerOperations,
			schedulingTopology);

		final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

		LOG.info(String.format(
			"Duration of scheduling tasks in streaming job is : %f ms",
			duration));
	}

	@Test
	public void testScheduleTasksInStreamingJobPerformance() {
		schedulerOperations = new TestingSchedulerOperations();
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

		final PipelinedRegionSchedulingStrategy schedulingStrategy = new PipelinedRegionSchedulingStrategy(
			schedulerOperations,
			schedulingTopology);

		final long startTime = System.nanoTime();

		schedulingStrategy.startScheduling();

		final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

		LOG.info(String.format(
			"Duration of scheduling tasks in streaming job is : %f ms",
			duration));
	}

	@Test
	public void testInitPipelinedRegionSchedulingStrategyInBatchJobPerformance() {
		schedulerOperations = new TestingSchedulerOperations();
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

		final long startTime = System.nanoTime();

		final PipelinedRegionSchedulingStrategy schedulingStrategy = new PipelinedRegionSchedulingStrategy(
			schedulerOperations,
			schedulingTopology);

		final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

		LOG.info(String.format(
			"Duration of scheduling tasks in batch job is : %f ms",
			duration));
	}

	@Test
	public void testScheduleSourceTasksInBatchJobPerformance() {
		schedulerOperations = new TestingSchedulerOperations();
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

		final PipelinedRegionSchedulingStrategy schedulingStrategy = new PipelinedRegionSchedulingStrategy(
			schedulerOperations,
			schedulingTopology);

		final long startTime = System.nanoTime();

		schedulingStrategy.startScheduling();

		final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

		LOG.info(String.format(
			"Duration of scheduling source tasks in batch job is : %f ms",
			duration));
	}

	@Test
	public void testSchedulingSinkTasksInBatchJobPerformance() {
		schedulerOperations = new TestingSchedulerOperations();
		schedulingTopology = new TestingSchedulingTopology();

		List<TestingSchedulingExecutionVertex> source =
			schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();
		List<TestingSchedulingExecutionVertex> sink =
			schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();

		schedulingTopology.connectAllToAll(source, sink)
			.withResultPartitionState(ResultPartitionState.CONSUMABLE)
			.withResultPartitionType(ResultPartitionType.BLOCKING)
			.finish();

		final PipelinedRegionSchedulingStrategy schedulingStrategy = new PipelinedRegionSchedulingStrategy(
			schedulerOperations,
			schedulingTopology);

		final long startTime = System.nanoTime();

		schedulingStrategy.onExecutionStateChange(sink.get(0).getId(), ExecutionState.FINISHED);

		final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

		LOG.info(String.format(
			"Duration of scheduling sink tasks in batch job is : %f ms",
			duration));
	}

}
