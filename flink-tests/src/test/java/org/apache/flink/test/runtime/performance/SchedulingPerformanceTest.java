package org.apache.flink.test.runtime.performance;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulerOperations;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.createAndInitExecutionGraph;

/**
 * Performance test for task scheduling.
 */
public class SchedulingPerformanceTest {

	private static final Logger LOG = LoggerFactory.getLogger(SchedulingPerformanceTest.class);

	private static final int PARALLELISM = 4000;

	private TestingSchedulerOperations schedulerOperations;
	private SchedulingTopology schedulingTopology;

	@Test
	public void testInitPipelinedRegionSchedulingStrategyInStreamingJobPerformance() throws Exception {
		schedulerOperations = new TestingSchedulerOperations();

		ExecutionGraph eg = createAndInitExecutionGraph(
			PARALLELISM,
			DistributionPattern.ALL_TO_ALL,
			ResultPartitionType.PIPELINED,
			ScheduleMode.EAGER,
			ExecutionMode.PIPELINED);

		schedulingTopology = eg.getSchedulingTopology();

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
	public void testInitPipelinedRegionSchedulingStrategyInBatchJobPerformance() throws Exception {
		schedulerOperations = new TestingSchedulerOperations();

		ExecutionGraph eg = createAndInitExecutionGraph(
			PARALLELISM,
			DistributionPattern.ALL_TO_ALL,
			ResultPartitionType.BLOCKING,
			ScheduleMode.LAZY_FROM_SOURCES,
			ExecutionMode.BATCH);

		schedulingTopology = eg.getSchedulingTopology();

		schedulingTopology.getAllPipelinedRegions();

		final long startTime = System.nanoTime();

		final PipelinedRegionSchedulingStrategy schedulingStrategy = new PipelinedRegionSchedulingStrategy(
			schedulerOperations,
			schedulingTopology);

		final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

		LOG.info(String.format(
			"Duration of scheduling tasks in batch job is : %f ms",
			duration));
	}
}
