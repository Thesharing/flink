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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulerOperations;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createAndInitExecutionGraph;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createDefaultJobVertices;

/** Performance test for task scheduling. */
public class SchedulingPerformanceTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulingPerformanceTest.class);

    private TestingSchedulerOperations schedulerOperations;
    private List<JobVertex> jobVertices;
    private ExecutionGraph executionGraph;
    private SchedulingTopology schedulingTopology;

    public void initSchedulingTopology(JobConfiguration jobConfiguration) throws Exception {
        schedulerOperations = new TestingSchedulerOperations();
        jobVertices = createDefaultJobVertices(jobConfiguration);
        executionGraph = createAndInitExecutionGraph(jobVertices, jobConfiguration);
        schedulingTopology = executionGraph.getSchedulingTopology();
    }

    @Test
    public void testInitPipelinedRegionSchedulingStrategyInStreamingJobPerformance()
            throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.STREAMING;

        initSchedulingTopology(jobConfiguration);

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
        JobConfiguration jobConfiguration = JobConfiguration.STREAMING;

        initSchedulingTopology(jobConfiguration);

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        final long startTime = System.nanoTime();

        schedulingStrategy.startScheduling();

        final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

        LOG.info(
                String.format(
                        "Duration of scheduling tasks in streaming job is : %f ms", duration));
    }

    @Test
    public void testInitPipelinedRegionSchedulingStrategyInBatchJobPerformance() throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.BATCH;

        initSchedulingTopology(jobConfiguration);

        final long startTime = System.nanoTime();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

        LOG.info(String.format("Duration of scheduling tasks in batch job is : %f ms", duration));
    }

    @Test
    public void testSchedulingSourceTaskInBatchJobPerformance() throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.BATCH;

        initSchedulingTopology(jobConfiguration);

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

        JobConfiguration jobConfiguration = JobConfiguration.BATCH;

        initSchedulingTopology(jobConfiguration);

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        // When we trying to scheduling downstream tasks via
        // onExecutionStateChange(ExecutionState.FINISHED),
        // the result partitions of upstream tasks need to be CONSUMABLE.
        // The CONSUMABLE status is determined by the variable "numberOfRunningProducers" of the
        // IntermediateResult.
        // Its value cannot be changed by any public methods.
        // So here we use reflections to modify this value and then schedule the downstream tasks.
        for (IntermediateResult result : executionGraph.getAllIntermediateResults().values()) {
            Field f = result.getClass().getDeclaredField("numberOfRunningProducers");
            f.setAccessible(true);
            AtomicInteger numberOfRunningProducers = (AtomicInteger) f.get(result);
            numberOfRunningProducers.set(0);
        }

        ExecutionVertexID executionVertexID =
                executionGraph
                        .getJobVertex(jobVertices.get(0).getID())
                        .getTaskVertices()[0]
                        .getID();

        final long startTime = System.nanoTime();

        schedulingStrategy.onExecutionStateChange(executionVertexID, ExecutionState.FINISHED);

        final double duration = (System.nanoTime() - startTime) / 1_000_000.0;

        LOG.info(
                String.format(
                        "Duration of scheduling sink tasks in batch job is : %f ms", duration));
    }
}
