package org.apache.flink.test.runtime.performance;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.DummyJobInformation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.NoOpExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.RegionPartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utilities for Runtime Performance Tests.
 */
public class RuntimePerformanceTestUtil {

	public static List<JobVertex> createDefaultJobVertices(
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

	public static JobGraph createJobGraph(
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

	public static ExecutionGraph createExecutionGraph(
		JobGraph jobGraph,
		SlotProvider slotProvider) throws IOException {

		final JobInformation jobInformation = new DummyJobInformation(
			jobGraph.getJobID(),
			jobGraph.getName());

		final ClassLoader classLoader = ExecutionGraph.class.getClassLoader();
		return new ExecutionGraph(
			jobInformation,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue(),
			slotProvider,
			classLoader,
			VoidBlobWriter.getInstance(),
			Time.seconds(10L),
			new RegionPartitionReleaseStrategy.Factory(),
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.INSTANCE,
			jobGraph.getScheduleMode(),
			NoOpExecutionDeploymentListener.INSTANCE,
			(execution, newState) -> {
			},
			System.currentTimeMillis());
	}

	public static List<SlotOffer> createSlotOffers(int slotNum) {
		return IntStream
			.range(0, slotNum)
			.mapToObj(i -> new SlotOffer(new AllocationID(), i, ResourceProfile.UNKNOWN))
			.limit(20)
			.collect(Collectors.toList());
	}

	public static ExecutionGraph createAndInitExecutionGraph(
		int parallelism,
		DistributionPattern distributionPattern,
		ResultPartitionType resultPartitionType,
		ScheduleMode scheduleMode,
		ExecutionMode executionMode) throws Exception {

		final List<JobVertex> jobVertices = createDefaultJobVertices(
			parallelism,
			distributionPattern,
			resultPartitionType);

		final JobGraph jobGraph = createJobGraph(
			jobVertices,
			scheduleMode,
			executionMode);

		final SlotProvider slotProvider = new SimpleSlotProvider(2 * parallelism);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		return eg;
	}

	public static SchedulingTopology createSchedulingTopology(
		int parallelism,
		DistributionPattern distributionPattern,
		ResultPartitionType resultPartitionType,
		ScheduleMode scheduleMode,
		ExecutionMode executionMode) throws Exception {
		ExecutionGraph eg = createAndInitExecutionGraph(
			parallelism,
			distributionPattern,
			resultPartitionType,
			scheduleMode,
			executionMode);
		return eg.getSchedulingTopology();
	}

	public static void startScheduling(final SchedulerNG scheduler) {
		scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		scheduler.startScheduling();
	}

	public static void waitForListFulfilled(
		Collection<?> list,
		int length,
		long maxWaitMillis) throws TimeoutException {

		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(maxWaitMillis));
		final Predicate<Collection<?>> predicate = (Collection<?> l) -> l.size() == length;
		boolean predicateResult;

		do {
			predicateResult = predicate.test(list);

			if (!predicateResult) {
				try {
					Thread.sleep(2L);
				} catch (InterruptedException ignored) {
					Thread.currentThread().interrupt();
				}
			}
		} while (!predicateResult && deadline.hasTimeLeft());

		if (!predicateResult) {
			throw new TimeoutException(String.format(
				"List no fulfilled in time, expected %d, actual %d.",
				length,
				list.size()));
		}
	}

	public static void deployTasks(
		ExecutionGraph executionGraph,
		JobVertexID jobVertexID,
		TestingLogicalSlotBuilder slotBuilder,
		boolean sendScheduleOrUpdateConsumersMessage) throws Exception {

		for (ExecutionVertex vertex : executionGraph.getJobVertex(jobVertexID).getTaskVertices()) {
			LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
			vertex.deployToSlot(slot);
			vertex.getCurrentExecutionAttempt()
				.registerProducedPartitions(
					slot.getTaskManagerLocation(),
					sendScheduleOrUpdateConsumersMessage).get();
		}
	}

	public static void deployAllTasks(
		ExecutionGraph executionGraph,
		TestingLogicalSlotBuilder slotBuilder) throws Exception {

		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
			vertex.deployToSlot(slot);
			vertex.getCurrentExecutionAttempt()
				.registerProducedPartitions(slot.getTaskManagerLocation(), true)
				.get();
		}
	}

	public static void transitionTaskStatus(
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

	public static void transitionTaskStatus(
		ExecutionGraph executionGraph,
		JobVertexID jobVertexID,
		ExecutionState state) {

		for (ExecutionVertex vertex : executionGraph
			.getJobVertex(jobVertexID)
			.getTaskVertices()) {
			executionGraph.updateState(new TaskExecutionStateTransition(new TaskExecutionState(
				executionGraph.getJobID(),
				vertex.getCurrentExecutionAttempt().getAttemptId(),
				state)));
		}
	}

	public static void transitionAllTaskStatus(
		ExecutionGraph executionGraph,
		ExecutionState state) {
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			executionGraph.updateState(new TaskExecutionStateTransition(new TaskExecutionState(
				executionGraph.getJobID(),
				vertex.getCurrentExecutionAttempt().getAttemptId(),
				state)));
		}
	}

	public static void transitionAllTaskStatus(
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
