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
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulerOperations;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Performance test for task scheduling. */
public class SchedulingPerformanceTest {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulingPerformanceTest.class);

    private static final int PARALLELISM = SchedulerPerformanceTestUtil.PARALLELISM;

    private TestingSchedulerOperations schedulerOperations;
    private SchedulingTopology schedulingTopology;

    @Test
    public void testInitPipelinedRegionSchedulingStrategyInStreamingJobPerformance()
            throws Exception {
        schedulerOperations = new TestingSchedulerOperations();

        schedulingTopology =
                SchedulerPerformanceTestUtil.createSchedulingTopology(
                        PARALLELISM,
                        DistributionPattern.ALL_TO_ALL,
                        ResultPartitionType.PIPELINED,
                        ScheduleMode.EAGER,
                        ExecutionMode.PIPELINED);

        final long startTime = System.nanoTime();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

        LOG.info(
                String.format(
                        "Duration of scheduling tasks in streaming job is : %f ms", duration));
    }

    @Test
    public void testSchedulingTasksInStreamingJobPerformance() throws Exception {
        schedulerOperations = new TestingSchedulerOperations();

        schedulingTopology =
                SchedulerPerformanceTestUtil.createSchedulingTopology(
                        PARALLELISM,
                        DistributionPattern.ALL_TO_ALL,
                        ResultPartitionType.PIPELINED,
                        ScheduleMode.EAGER,
                        ExecutionMode.PIPELINED);

        final long startTime = System.nanoTime();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

        LOG.info(
                String.format(
                        "Duration of scheduling tasks in streaming job is : %f ms", duration));
    }

    @Test
    public void testInitPipelinedRegionSchedulingStrategyInBatchJobPerformance() throws Exception {
        schedulerOperations = new TestingSchedulerOperations();

        schedulingTopology =
                SchedulerPerformanceTestUtil.createSchedulingTopology(
                        PARALLELISM,
                        DistributionPattern.ALL_TO_ALL,
                        ResultPartitionType.BLOCKING,
                        ScheduleMode.LAZY_FROM_SOURCES,
                        ExecutionMode.BATCH);

        schedulingTopology.getAllPipelinedRegions();

        final long startTime = System.nanoTime();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

        LOG.info(String.format("Duration of scheduling tasks in batch job is : %f ms", duration));
    }

    @Test
    public void testSchedulingSourceTaskInBatchJobPerformance() throws Exception {
        schedulerOperations = new TestingSchedulerOperations();

        schedulingTopology =
                SchedulerPerformanceTestUtil.createSchedulingTopology(
                        PARALLELISM,
                        DistributionPattern.ALL_TO_ALL,
                        ResultPartitionType.BLOCKING,
                        ScheduleMode.LAZY_FROM_SOURCES,
                        ExecutionMode.BATCH);

        schedulingTopology.getAllPipelinedRegions();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        final long startTime = System.nanoTime();

        schedulingStrategy.startScheduling();

        final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

        LOG.info(
                String.format(
                        "Duration of scheduling source tasks in batch job is : %f ms", duration));
    }

    @Test
    public void testSchedulingSinkTaskInBatchJobPerformance() throws Exception {
        schedulerOperations = new TestingSchedulerOperations();

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

        schedulingTopology.getAllPipelinedRegions();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        for (IntermediateResult result : eg.getAllIntermediateResults().values()) {
            Field f = result.getClass().getDeclaredField("numberOfRunningProducers");
            f.setAccessible(true);
            AtomicInteger numberOfRunningProducers = (AtomicInteger) f.get(result);
            numberOfRunningProducers.set(0);
        }

        ExecutionVertexID executionVertexID =
                eg.getJobVertex(jobVertices.get(0).getID()).getTaskVertices()[0].getID();

        final long startTime = System.nanoTime();

        schedulingStrategy.onExecutionStateChange(executionVertexID, ExecutionState.FINISHED);

        final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

        LOG.info(
                String.format(
                        "Duration of scheduling sink tasks in batch job is : %f ms", duration));
    }
}
