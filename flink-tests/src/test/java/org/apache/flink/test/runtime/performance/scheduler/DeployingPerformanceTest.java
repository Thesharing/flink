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

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.rpc.messages.RemoteRpcInvocation;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.test.runtime.performance.scheduler.MemoryUtil.convertToMiB;
import static org.apache.flink.test.runtime.performance.scheduler.MemoryUtil.getHeapMemory;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createAndInitExecutionGraph;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createDefaultJobVertices;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.waitForListFulfilled;

/** Performance tests for deploying tasks. */
public class DeployingPerformanceTest {
    private static final Logger LOG = LoggerFactory.getLogger(DeployingPerformanceTest.class);

    private List<JobVertex> jobVertices;
    private ExecutionGraph executionGraph;
    private BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors;

    public void createAndSetupExecutionGraph(JobConfiguration jobConfiguration) throws Exception {

        jobVertices = createDefaultJobVertices(jobConfiguration);

        executionGraph = createAndInitExecutionGraph(jobVertices, jobConfiguration);

        taskDeploymentDescriptors = new ArrayBlockingQueue<>(jobConfiguration.getParallelism() * 2);

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        taskManagerGateway.setSubmitConsumer(taskDeploymentDescriptors::offer);

        final TestingLogicalSlotBuilder slotBuilder =
                new TestingLogicalSlotBuilder().setTaskManagerGateway(taskManagerGateway);

        for (ExecutionJobVertex ejv : executionGraph.getVerticesTopologically()) {
            for (ExecutionVertex ev : ejv.getTaskVertices()) {
                final LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
                final Execution execution = ev.getCurrentExecutionAttempt();
                execution.registerProducedPartitions(slot.getTaskManagerLocation(), true).get();
                if (!execution.tryAssignResource(slot)) {
                    throw new RuntimeException("Error when assigning slot to execution.");
                }
            }
        }
    }

    @Test
    public void testDeployInStreamingTaskPerformance() throws Exception {

        JobConfiguration configuration = JobConfiguration.STREAMING;

        createAndSetupExecutionGraph(configuration);

        final long startTime = System.nanoTime();

        for (ExecutionJobVertex ejv : executionGraph.getVerticesTopologically()) {
            for (ExecutionVertex ev : ejv.getTaskVertices()) {
                Execution execution = ev.getCurrentExecutionAttempt();
                execution.deploy();
            }
        }
        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        waitForListFulfilled(taskDeploymentDescriptors, configuration.getParallelism() * 2, 1000L);

        LOG.info(
                String.format(
                        "Duration of deploying tasks in streaming task is : %d ms", duration));
    }

    @Test
    public void testDeployInBatchTaskPerformance() throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.BATCH;

        createAndSetupExecutionGraph(jobConfiguration);

        final JobVertex source = jobVertices.get(0);

        MemoryUsage startMemoryUsage = getHeapMemory();

        final long sourceStartTime = System.nanoTime();

        for (ExecutionVertex ev : executionGraph.getJobVertex(source.getID()).getTaskVertices()) {
            Execution execution = ev.getCurrentExecutionAttempt();
            execution.deploy();
        }

        final long sourceDuration = (System.nanoTime() - sourceStartTime) / 1_000_000;

        MemoryUsage endMemoryUsage = getHeapMemory();

        final float sourceHeapUsed =
                convertToMiB(endMemoryUsage.getUsed() - startMemoryUsage.getUsed());

        waitForListFulfilled(taskDeploymentDescriptors, jobConfiguration.getParallelism(), 1000L);

        final JobVertex sink = jobVertices.get(1);

        startMemoryUsage = getHeapMemory();

        final long sinkStartTime = System.nanoTime();

        for (ExecutionVertex ev : executionGraph.getJobVertex(sink.getID()).getTaskVertices()) {
            Execution execution = ev.getCurrentExecutionAttempt();
            execution.deploy();
        }

        final long sinkDuration = (System.nanoTime() - sinkStartTime) / 1_000_000;

        endMemoryUsage = getHeapMemory();

        final float sinkHeapUsed =
                convertToMiB(endMemoryUsage.getUsed() - startMemoryUsage.getUsed());

        SchedulerPerformanceTestUtil.waitForListFulfilled(
                taskDeploymentDescriptors, jobConfiguration.getParallelism() * 2, 1000L);

        LOG.info(String.format("Duration of deploying source tasks is: %d ms", sourceDuration));
        LOG.info(String.format("Duration of deploying sink tasks is: %d ms", sinkDuration));
        LOG.info(
                String.format(
                        "Heap memory usage of deploying source tasks is: %f MiB", sourceHeapUsed));
        LOG.info(
                String.format(
                        "Heap memory usage of deploying sink tasks is: %f MiB", sinkHeapUsed));
    }

    @Test
    public void testSubmitTaskInBatchTaskPerformance() throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.BATCH;

        createAndSetupExecutionGraph(jobConfiguration);

        Class<?>[] types = {TaskDeploymentDescriptor.class};

        JobVertex source = jobVertices.get(0);

        MemoryUsage startMemoryUsage = getHeapMemory();

        List<RemoteRpcInvocation> sourceInvocation =
                Collections.synchronizedList(new ArrayList<>(jobConfiguration.getParallelism()));

        final long sourceStartTime = System.nanoTime();

        for (ExecutionVertex ev : executionGraph.getJobVertex(source.getID()).getTaskVertices()) {
            Execution execution = ev.getCurrentExecutionAttempt();
            Object[] args = {execution.createTaskDeploymentDescriptor()};
            CompletableFuture.runAsync(
                    () -> {
                        try {
                            sourceInvocation.add(
                                    new RemoteRpcInvocation("submitTask", types, args));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    },
                    executionGraph.getFutureExecutor());
        }

        final long sourceDuration = (System.nanoTime() - sourceStartTime) / 1_000_000;

        MemoryUsage endMemoryUsage = getHeapMemory();

        final float sourceHeapUsed =
                convertToMiB(endMemoryUsage.getUsed() - startMemoryUsage.getUsed());

        LOG.info(String.format("Duration of serializing source tasks is: %d ms", sourceDuration));
        LOG.info(
                String.format(
                        "Heap memory usage of serializing source tasks is: %f MiB",
                        sourceHeapUsed));

        JobVertex sink = jobVertices.get(1);

        startMemoryUsage = getHeapMemory();

        List<RemoteRpcInvocation> sinkInvocations =
                Collections.synchronizedList(new ArrayList<>(jobConfiguration.getParallelism()));

        final long sinkStartTime = System.nanoTime();

        for (ExecutionVertex ev : executionGraph.getJobVertex(sink.getID()).getTaskVertices()) {
            Execution execution = ev.getCurrentExecutionAttempt();
            Object[] args = {execution.createTaskDeploymentDescriptor()};
            CompletableFuture.runAsync(
                    () -> {
                        try {
                            sinkInvocations.add(new RemoteRpcInvocation("submitTask", types, args));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }

        final long sinkDuration = (System.nanoTime() - sinkStartTime) / 1_000_000;

        endMemoryUsage = getHeapMemory();

        final float sinkHeapUsed =
                convertToMiB(endMemoryUsage.getUsed() - startMemoryUsage.getUsed());

        LOG.info(String.format("Duration of serializing sink tasks is: %d ms", sinkDuration));
        LOG.info(
                String.format(
                        "Heap memory usage of serializing sink tasks is: %f MiB", sinkHeapUsed));
    }
}
