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
 * limitations under the License
 */

package org.apache.flink.test.runtime.performance.scheduler;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/** Utilities for Runtime Performance Tests. */
public class SchedulerPerformanceTestUtil extends TestLogger {

    public static List<JobVertex> createDefaultJobVertices(JobConfiguration jobConfiguration) {

        final List<JobVertex> jobVertices = new ArrayList<>();

        final JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(jobConfiguration.getParallelism());
        jobVertices.add(source);

        final JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);
        sink.setParallelism(jobConfiguration.getParallelism());
        jobVertices.add(sink);

        sink.connectNewDataSetAsInput(
                source,
                jobConfiguration.getDistributionPattern(),
                jobConfiguration.getResultPartitionType());

        return jobVertices;
    }

    public static JobGraph createJobGraph(JobConfiguration jobConfiguration) throws IOException {
        return createJobGraph(Collections.emptyList(), jobConfiguration);
    }

    public static JobGraph createJobGraph(
            List<JobVertex> jobVertices, JobConfiguration jobConfiguration) throws IOException {

        final JobGraph jobGraph = new JobGraph(jobVertices.toArray(new JobVertex[0]));

        jobGraph.setScheduleMode(jobConfiguration.getScheduleMode());

        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setExecutionMode(jobConfiguration.getExecutionMode());
        jobGraph.setExecutionConfig(executionConfig);

        return jobGraph;
    }

    public static TestingComponentMainThreadExecutor createMainThreadExecutor() {
        final ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor();

        return new TestingComponentMainThreadExecutor(
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService));
    }

    public static ExecutionGraph createAndInitExecutionGraph(
            List<JobVertex> jobVertices,
            JobConfiguration jobConfiguration,
            ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {

        final JobGraph jobGraph = createJobGraph(jobVertices, jobConfiguration);

        final DefaultScheduler scheduler =
                SchedulerTestingUtils.createScheduler(jobGraph, mainThreadExecutor);

        return scheduler.getExecutionGraph();
    }

    public static ExecutionGraph createAndInitExecutionGraph(
            List<JobVertex> jobVertices, JobConfiguration jobConfiguration) throws Exception {

        return createAndInitExecutionGraph(
                jobVertices,
                jobConfiguration,
                ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }

    public static void waitForListFulfilled(Collection<?> list, int length, long maxWaitMillis)
            throws TimeoutException {

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
            throw new TimeoutException(
                    String.format(
                            "List no fulfilled in time, expected %d, actual %d.",
                            length, list.size()));
        }
    }

    public static void deployTasks(
            ExecutionGraph executionGraph,
            JobVertexID jobVertexID,
            TestingLogicalSlotBuilder slotBuilder,
            boolean sendScheduleOrUpdateConsumersMessage)
            throws JobException, ExecutionException, InterruptedException {

        for (ExecutionVertex vertex : executionGraph.getJobVertex(jobVertexID).getTaskVertices()) {
            LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
            Execution execution = vertex.getCurrentExecutionAttempt();
            execution
                    .registerProducedPartitions(
                            slot.getTaskManagerLocation(), sendScheduleOrUpdateConsumersMessage)
                    .get();
            assignResourceAndDeploy(vertex, slot);
        }
    }

    public static void deployAllTasks(
            ExecutionGraph executionGraph, TestingLogicalSlotBuilder slotBuilder)
            throws JobException, ExecutionException, InterruptedException {

        for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
            LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
            vertex.getCurrentExecutionAttempt()
                    .registerProducedPartitions(slot.getTaskManagerLocation(), true)
                    .get();
            assignResourceAndDeploy(vertex, slot);
        }
    }

    private static void assignResourceAndDeploy(ExecutionVertex vertex, LogicalSlot slot)
            throws JobException {
        vertex.tryAssignResource(slot);
        vertex.deploy();
    }

    public static void transitionTaskStatus(
            ExecutionGraph executionGraph, JobVertexID jobVertexID, ExecutionState state) {

        for (ExecutionVertex vertex : executionGraph.getJobVertex(jobVertexID).getTaskVertices()) {
            executionGraph.updateState(
                    new TaskExecutionStateTransition(
                            new TaskExecutionState(
                                    executionGraph.getJobID(),
                                    vertex.getCurrentExecutionAttempt().getAttemptId(),
                                    state)));
        }
    }

    public static void transitionAllTaskStatus(
            ExecutionGraph executionGraph, ExecutionState state) {
        for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
            executionGraph.updateState(
                    new TaskExecutionStateTransition(
                            new TaskExecutionState(
                                    executionGraph.getJobID(),
                                    vertex.getCurrentExecutionAttempt().getAttemptId(),
                                    state)));
        }
    }
}
