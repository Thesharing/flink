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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/** Performance test for region failover. */
public class RegionFailoverPerformanceTest {
    private static final Logger LOG = LoggerFactory.getLogger(RegionFailoverPerformanceTest.class);

    public static final int PARALLELISM = SchedulerPerformanceTestUtil.PARALLELISM;

    private SchedulingTopology schedulingTopology;

    @Test
    public void testCalculateRegionToRestartInStreamingJobPerformance() throws Exception {
        final List<JobVertex> jobVertices =
                SchedulerPerformanceTestUtil.createDefaultJobVertices(
                        PARALLELISM, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        final JobGraph jobGraph =
                SchedulerPerformanceTestUtil.createJobGraph(
                        jobVertices, ScheduleMode.EAGER, ExecutionMode.PIPELINED);

        final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

        final ExecutionGraph eg =
                SchedulerPerformanceTestUtil.createExecutionGraph(jobGraph, slotProvider);

        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

        schedulingTopology = eg.getSchedulingTopology();

        JobVertex source = jobVertices.get(0);

        TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();
        SchedulerPerformanceTestUtil.deployAllTasks(eg, slotBuilder);
        SchedulerPerformanceTestUtil.transitionAllTaskStatus(eg, ExecutionState.RUNNING);

        RestartPipelinedRegionFailoverStrategy strategy =
                new RestartPipelinedRegionFailoverStrategy(schedulingTopology);

        final long startTime = System.nanoTime();

        Set<ExecutionVertexID> tasks =
                strategy.getTasksNeedingRestart(
                        eg.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
                        new Exception("For test."));

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));

        if (tasks.size() != PARALLELISM * 2) {
            throw new RuntimeException(
                    String.format(
                            "Number of tasks to restart mismatch, expected %d, actual %d.",
                            PARALLELISM * 2, tasks.size()));
        }
    }

    @Test
    public void testCalculateRegionToRestartInBatchJobPerformance() throws Exception {
        final List<JobVertex> jobVertices =
                SchedulerPerformanceTestUtil.createDefaultJobVertices(
                        PARALLELISM, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        final JobGraph jobGraph =
                SchedulerPerformanceTestUtil.createJobGraph(
                        jobVertices, ScheduleMode.LAZY_FROM_SOURCES, ExecutionMode.BATCH);
        final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

        final ExecutionGraph eg =
                SchedulerPerformanceTestUtil.createExecutionGraph(jobGraph, slotProvider);

        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

        schedulingTopology = eg.getSchedulingTopology();

        TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        JobVertex source = jobVertices.get(0);
        SchedulerPerformanceTestUtil.deployTasks(eg, source.getID(), slotBuilder, false);
        SchedulerPerformanceTestUtil.transitionTaskStatus(
                eg, source.getID(), ExecutionState.FINISHED);

        JobVertex sink = jobVertices.get(1);
        SchedulerPerformanceTestUtil.deployTasks(eg, sink.getID(), slotBuilder, false);
        SchedulerPerformanceTestUtil.transitionTaskStatus(eg, sink.getID(), ExecutionState.RUNNING);

        RestartPipelinedRegionFailoverStrategy strategy =
                new RestartPipelinedRegionFailoverStrategy(schedulingTopology);

        final long startTime = System.nanoTime();

        Set<ExecutionVertexID> tasks =
                strategy.getTasksNeedingRestart(
                        eg.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
                        new Exception("For test."));

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));

        if (tasks.size() != PARALLELISM + 1) {
            throw new RuntimeException(
                    String.format(
                            "Number of tasks to restart mismatch, expected %d, actual %d.",
                            PARALLELISM + 1, tasks.size()));
        }
    }
}
