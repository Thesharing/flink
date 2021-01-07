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

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** Performance tests for deploying tasks. */
public class DeployingPerformanceTest {
    private static final Logger LOG = LoggerFactory.getLogger(DeployingPerformanceTest.class);

    public static final int PARALLELISM = SchedulerPerformanceTestUtil.PARALLELISM;

    @Test
    public void testDeployInStreamingTaskPerformance() throws Exception {
        final List<JobVertex> jobVertices =
                SchedulerPerformanceTestUtil.createDefaultJobVertices(
                        PARALLELISM, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        final JobGraph jobGraph =
                SchedulerPerformanceTestUtil.createJobGraph(
                        jobVertices, ScheduleMode.EAGER, ExecutionMode.PIPELINED);

        final BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors =
                new ArrayBlockingQueue<>(PARALLELISM * 2);
        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        taskManagerGateway.setSubmitConsumer(taskDeploymentDescriptors::offer);

        final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

        final ExecutionGraph eg =
                SchedulerPerformanceTestUtil.createExecutionGraph(jobGraph, slotProvider);
        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

        TestingLogicalSlotBuilder slotBuilder =
                new TestingLogicalSlotBuilder().setTaskManagerGateway(taskManagerGateway);
        for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
            for (ExecutionVertex ev : ejv.getTaskVertices()) {
                Execution execution = ev.getCurrentExecutionAttempt();
                LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
                execution.registerProducedPartitions(slot.getTaskManagerLocation(), true).get();
                if (!execution.tryAssignResource(slot)) {
                    throw new RuntimeException("Error when assigning slot to execution.");
                }
            }
        }

        final long startTime = System.nanoTime();

        for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
            for (ExecutionVertex ev : ejv.getTaskVertices()) {
                Execution execution = ev.getCurrentExecutionAttempt();
                execution.deploy();
            }
        }

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        SchedulerPerformanceTestUtil.waitForListFulfilled(
                taskDeploymentDescriptors, PARALLELISM * 2, 1000L);

        LOG.info(
                String.format(
                        "Duration of deploying tasks in streaming task is : %d ms", duration));
    }

    @Test
    public void testDeployInBatchTaskPerformance() throws Exception {
        final List<JobVertex> jobVertices =
                SchedulerPerformanceTestUtil.createDefaultJobVertices(
                        PARALLELISM, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        final JobGraph jobGraph =
                SchedulerPerformanceTestUtil.createJobGraph(
                        jobVertices, ScheduleMode.LAZY_FROM_SOURCES, ExecutionMode.BATCH);

        final BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors =
                new ArrayBlockingQueue<>(PARALLELISM * 2);
        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        taskManagerGateway.setSubmitConsumer(taskDeploymentDescriptors::offer);

        final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

        final ExecutionGraph eg =
                SchedulerPerformanceTestUtil.createExecutionGraph(jobGraph, slotProvider);
        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

        TestingLogicalSlotBuilder slotBuilder =
                new TestingLogicalSlotBuilder().setTaskManagerGateway(taskManagerGateway);
        for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
            for (ExecutionVertex ev : ejv.getTaskVertices()) {
                Execution execution = ev.getCurrentExecutionAttempt();
                LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
                execution.registerProducedPartitions(slot.getTaskManagerLocation(), true).get();
                if (!execution.tryAssignResource(slot)) {
                    throw new RuntimeException("Error when assigning slot to execution.");
                }
            }
        }

        JobVertex source = jobVertices.get(0);

        final long sourceStartTime = System.nanoTime();

        for (ExecutionVertex ev : eg.getJobVertex(source.getID()).getTaskVertices()) {
            Execution execution = ev.getCurrentExecutionAttempt();
            execution.deploy();
        }

        final long sourceDuration = (System.nanoTime() - sourceStartTime) / 1_000_000;

        SchedulerPerformanceTestUtil.waitForListFulfilled(
                taskDeploymentDescriptors, PARALLELISM, 1000L);

        JobVertex sink = jobVertices.get(1);

        final long sinkStartTime = System.nanoTime();

        for (ExecutionVertex ev : eg.getJobVertex(sink.getID()).getTaskVertices()) {
            Execution execution = ev.getCurrentExecutionAttempt();
            execution.deploy();
        }

        final long sinkDuration = (System.nanoTime() - sinkStartTime) / 1_000_000;

        SchedulerPerformanceTestUtil.waitForListFulfilled(
                taskDeploymentDescriptors, PARALLELISM * 2, 1000L);

        LOG.info(String.format("Duration of deploying source tasks is : %d ms", sourceDuration));
        LOG.info(String.format("Duration of deploying sink tasks is : %d ms", sinkDuration));
    }
}
