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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.DefaultExecutionVertexOperations;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestExecutionVertexOperationsDecorator;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Performance tests.
 */
public class PerformanceTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final long SUBMIT_TIMEOUT = 300_000L;

	private static final int PARALLELISM = 4000;

	private ExecutorService executor;
	private ScheduledExecutorService scheduledExecutorService;
	private Configuration configuration;
	private TestRestartBackoffTimeStrategy testRestartBackoffTimeStrategy;
	private TestExecutionVertexOperationsDecorator testExecutionVertexOperations;
	private ExecutionVertexVersioner executionVertexVersioner;
	private ManuallyTriggeredScheduledExecutor taskRestartExecutor = new ManuallyTriggeredScheduledExecutor();

	@Before
	public void setup() {
		executor = Executors.newSingleThreadExecutor();
		scheduledExecutorService = new DirectScheduledExecutorService();
		configuration = new Configuration();
		testRestartBackoffTimeStrategy = new TestRestartBackoffTimeStrategy(true, 0);
		testExecutionVertexOperations = new TestExecutionVertexOperationsDecorator(new DefaultExecutionVertexOperations());
		executionVertexVersioner = new ExecutionVertexVersioner();
	}

	@Test
	public void testDeployTaskInStreamingJobPerformance() throws Exception {
		final List<JobVertex> jobVertices = createDefaultJobVertices(
			PARALLELISM,
			DistributionPattern.ALL_TO_ALL,
			ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = createJobGraph(
			jobVertices,
			ScheduleMode.EAGER,
			ExecutionMode.PIPELINED);

		final BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors = new ArrayBlockingQueue<>(
			PARALLELISM * 2);
		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		taskManagerGateway.setSubmitConsumer(taskDeploymentDescriptors::offer);
		final SlotProvider slotProvider = new SimpleSlotProvider(
			PARALLELISM * 2,
			taskManagerGateway);

		final DefaultScheduler scheduler = createScheduler(
			jobGraph,
			slotProvider);

		final long startTime = System.nanoTime();

		startScheduling(scheduler);

		waitForAllTaskSubmitted(taskDeploymentDescriptors, PARALLELISM * 2, SUBMIT_TIMEOUT);

		final long duration = (System.nanoTime() - startTime) / 1_000_000;

		LOG.info(String.format("Duration of deploying tasks is : %d ms", duration));
	}

	@Test
	public void testDeployTaskInBatchJobPerformance() throws Exception {
		final List<JobVertex> jobVertices = createDefaultJobVertices(
			PARALLELISM,
			DistributionPattern.ALL_TO_ALL,
			ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = createJobGraph(
			jobVertices,
			ScheduleMode.LAZY_FROM_SOURCES,
			ExecutionMode.BATCH);

		final BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors =
			new ArrayBlockingQueue<>(PARALLELISM * 2);
		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		taskManagerGateway.setSubmitConsumer(taskDeploymentDescriptors::offer);
		final SlotProvider slotProvider = new SimpleSlotProvider(
			PARALLELISM * 2,
			taskManagerGateway);

		final DefaultScheduler scheduler = createScheduler(
			jobGraph,
			slotProvider);

		final long startTime = System.nanoTime();

		startScheduling(scheduler);

		waitForAllTaskSubmitted(taskDeploymentDescriptors, PARALLELISM, SUBMIT_TIMEOUT);

		final long sourceDuration = (System.nanoTime() - startTime) / 1_000_000;

		final JobVertex source = jobVertices.get(0);

		final AccessExecutionJobVertex ejv = scheduler
			.requestJob()
			.getAllVertices()
			.get(source.getID());

		for (int i = 0; i < PARALLELISM - 1; i++) {
			transitionTaskStatus(scheduler, ejv, i, ExecutionState.FINISHED);
		}

		final long sinkStartTime = System.nanoTime();

		transitionTaskStatus(scheduler, ejv, PARALLELISM - 1, ExecutionState.FINISHED);

		waitForAllTaskSubmitted(taskDeploymentDescriptors, PARALLELISM * 2, SUBMIT_TIMEOUT);

		long sinkDuration = (System.nanoTime() - sinkStartTime) / 1_000_000;

		LOG.info(String.format("Duration of deploying sources is : %d ms", sourceDuration));
		LOG.info(String.format("Duration of deploying sinks is : %d ms", sinkDuration));
	}

	@Test
	public void testRegionFailoverInStreamingJobPerformance() throws Exception {
		final List<JobVertex> jobVertices = createDefaultJobVertices(
			PARALLELISM,
			DistributionPattern.ALL_TO_ALL,
			ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = createJobGraph(
			jobVertices,
			ScheduleMode.EAGER,
			ExecutionMode.PIPELINED);

		final BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors = new ArrayBlockingQueue<>(
			PARALLELISM * 2);
		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		taskManagerGateway.setSubmitConsumer(taskDeploymentDescriptors::offer);
		final SlotProvider slotProvider = new SimpleSlotProvider(
			PARALLELISM * 2,
			taskManagerGateway);

		final DefaultScheduler scheduler = createScheduler(
			jobGraph,
			slotProvider);

		startScheduling(scheduler);

		final JobVertex source = jobVertices.get(0);
		final JobVertex sink = jobVertices.get(1);

		Predicate<AccessExecution> isDeploying = ExecutionGraphTestUtils.isInExecutionState(
			ExecutionState.DEPLOYING);
		ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
			scheduler.getExecutionGraph(),
			isDeploying,
			TIMEOUT.toMilliseconds());

		final AccessExecutionJobVertex ejvSource =
			scheduler.requestJob().getAllVertices().get(source.getID());
		transitionAllTaskStatus(scheduler, ejvSource, ExecutionState.RUNNING);
		final AccessExecutionJobVertex ejvSink =
			scheduler.requestJob().getAllVertices().get(sink.getID());
		transitionAllTaskStatus(scheduler, ejvSink, ExecutionState.RUNNING);

		ExecutionVertex ev11 = scheduler.getExecutionJobVertex(source.getID()).getTaskVertices()[0];

		final long startTime = System.nanoTime();
		scheduler.updateTaskExecutionState(new TaskExecutionState(
			jobGraph.getJobID(),
			ev11.getCurrentExecutionAttempt().getAttemptId(),
			ExecutionState.FAILED));
		final long duration = (System.nanoTime() - startTime) / 1_000_000;
		LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));
	}

	public List<JobVertex> createDefaultJobVertices(
		int parallelism,
		DistributionPattern distributionPattern,
		ResultPartitionType resultPartitionType) {

		List<JobVertex> jobVertices = new ArrayList<>();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(parallelism);
		jobVertices.add(source);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(parallelism);
		jobVertices.add(sink);

		sink.connectNewDataSetAsInput(source, distributionPattern, resultPartitionType);

		return jobVertices;
	}

	public JobGraph createJobGraph(
		List<JobVertex> jobVertices,
		ScheduleMode scheduleMode,
		ExecutionMode executionMode) throws IOException {

		final JobGraph jobGraph = new JobGraph(jobVertices.toArray(new JobVertex[0]));

		jobGraph.setScheduleMode(scheduleMode);
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setExecutionMode(executionMode);
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}

	public List<SlotOffer> createSlotOffers(int slotNum) {
		return IntStream
			.range(0, slotNum)
			.mapToObj(i -> new SlotOffer(new AllocationID(), i, ResourceProfile.UNKNOWN))
			.limit(20)
			.collect(Collectors.toList());
	}

	private DefaultScheduler createScheduler(
		final JobGraph jobGraph,
		final SlotProvider slotProvider) throws Exception {

		return SchedulerTestingUtils.newSchedulerBuilderWithDefaultSlotAllocator(
			jobGraph,
			slotProvider,
			TIMEOUT)
			.setLogger(log)
			.setIoExecutor(executor)
			.setJobMasterConfiguration(configuration)
			.setFutureExecutor(scheduledExecutorService)
			.setDelayExecutor(taskRestartExecutor)
			.setSchedulingStrategyFactory(new PipelinedRegionSchedulingStrategy.Factory())
			.setRestartBackoffTimeStrategy(testRestartBackoffTimeStrategy)
			.setExecutionVertexOperations(testExecutionVertexOperations)
			.setExecutionVertexVersioner(executionVertexVersioner)
			.build();
	}

	private void startScheduling(final SchedulerNG scheduler) {
		scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		scheduler.startScheduling();
	}

	public static void waitForAllTaskSubmitted(
		BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors,
		int desiredCount,
		long maxWaitMillis
	) throws TimeoutException {
		// this is a poor implementation - we may want to improve it eventually
		final long deadline =
			maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (taskDeploymentDescriptors.size() < desiredCount && System.nanoTime() < deadline) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException ignored) {
			}
		}

		if (System.nanoTime() >= deadline) {
			throw new TimeoutException();
		}
	}

	private static void transitionTaskStatus(
		DefaultScheduler scheduler,
		AccessExecutionJobVertex vertex,
		int subtask,
		ExecutionState executionState) {

		final ExecutionAttemptID attemptId = vertex.getTaskVertices()[subtask]
			.getCurrentExecutionAttempt()
			.getAttemptId();
		scheduler.updateTaskExecutionState(
			new TaskExecutionState(
				scheduler.getJobGraph().getJobID(),
				attemptId,
				executionState));
	}

	private static void transitionAllTaskStatus(
		DefaultScheduler scheduler,
		AccessExecutionJobVertex vertex,
		ExecutionState executionState) {

		for (AccessExecutionVertex ev : vertex.getTaskVertices()) {
			ExecutionAttemptID attemptId = ev.getCurrentExecutionAttempt().getAttemptId();
			scheduler.updateTaskExecutionState(
				new TaskExecutionState(
					scheduler.getJobGraph().getJobID(),
					attemptId,
					executionState));
		}
	}
}
