package org.apache.flink.test.runtime.performance;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.createDefaultJobVertices;
import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.createExecutionGraph;
import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.createJobGraph;
import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.deployAllTasks;
import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.deployTasks;
import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.transitionAllTaskStatus;
import static org.apache.flink.test.runtime.performance.RuntimePerformanceTestUtil.transitionTaskStatus;

/**
 * Performance test for region failover.
 */
public class RegionFailoverPerformanceTest {
	private static final Logger LOG = LoggerFactory.getLogger(DeployingPerformanceTest.class);

	public static final int PARALLELISM = 8000;

	private SchedulingTopology schedulingTopology;

	@Test
	public void testCalculateRegionToRestartInStreamingJobPerformance() throws Exception {
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

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		schedulingTopology = eg.getSchedulingTopology();

		JobVertex source = jobVertices.get(0);

		TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();
		deployAllTasks(eg, slotBuilder);
		transitionAllTaskStatus(eg, ExecutionState.RUNNING);

		RestartPipelinedRegionFailoverStrategy strategy =
			new RestartPipelinedRegionFailoverStrategy(schedulingTopology);

		final long startTime = System.nanoTime();

		Set<ExecutionVertexID> tasks = strategy.getTasksNeedingRestart(
			eg.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
			new Exception("For test."));

		final long duration = (System.nanoTime() - startTime) / 1_000_000;

		LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));

		if (tasks.size() != PARALLELISM * 2) {
			throw new RuntimeException(String.format(
				"Number of tasks to restart mismatch, expected %d, actual %d.",
				PARALLELISM * 2,
				tasks.size()));
		}
	}

	@Test
	public void testCalculateRegionToRestartInBatchJobPerformance() throws Exception {
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

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		schedulingTopology = eg.getSchedulingTopology();

		TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

		JobVertex source = jobVertices.get(0);
		deployTasks(eg, source.getID(), slotBuilder, false);
		transitionTaskStatus(eg, source.getID(), ExecutionState.FINISHED);

		JobVertex sink = jobVertices.get(1);
		deployTasks(eg, sink.getID(), slotBuilder, false);
		transitionTaskStatus(eg, sink.getID(), ExecutionState.RUNNING);

		RestartPipelinedRegionFailoverStrategy strategy =
			new RestartPipelinedRegionFailoverStrategy(schedulingTopology);

		final long startTime = System.nanoTime();

		Set<ExecutionVertexID> tasks = strategy.getTasksNeedingRestart(
			eg.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
			new Exception("For test."));

		final long duration = (System.nanoTime() - startTime) / 1_000_000;

		LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));

		if (tasks.size() != PARALLELISM + 1) {
			throw new RuntimeException(String.format(
				"Number of tasks to restart mismatch, expected %d, actual %d.",
				PARALLELISM + 1,
				tasks.size()));
		}
	}

	public static void waitForAllExecutionsPredicate(
		ExecutionGraph executionGraph,
		JobVertexID jobVertexID,
		Predicate<AccessExecution> executionPredicate,
		long maxWaitMillis) throws TimeoutException {

		ExecutionVertex[] executionVertices = executionGraph.getJobVertex(jobVertexID).getTaskVertices();
		final Predicate<ExecutionVertex[]> allExecutionsPredicate = allExecutionsPredicate(executionPredicate);
		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(maxWaitMillis));
		boolean predicateResult;

		do {
			predicateResult = allExecutionsPredicate.test(executionVertices);

			if (!predicateResult) {
				try {
					Thread.sleep(2L);
				} catch (InterruptedException ignored) {
					Thread.currentThread().interrupt();
				}
			}
		} while (!predicateResult && deadline.hasTimeLeft());

		if (!predicateResult) {
			throw new TimeoutException("Not all executions fulfilled the predicate in time.");
		}
	}

	public static Predicate<ExecutionVertex[]> allExecutionsPredicate(final Predicate<AccessExecution> executionPredicate) {
		return executionVertices -> {
			for (AccessExecutionVertex executionVertex : executionVertices) {
				final AccessExecution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();

				if (currentExecutionAttempt == null || !executionPredicate.test(currentExecutionAttempt)) {
					return false;
				}
			}
			return true;
		};
	}
}
