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
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.switchAllVerticesToRunning;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createAndInitExecutionGraph;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createDefaultJobVertices;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.deployAllTasks;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.deployTasks;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.transitionTaskStatus;

/** Performance test for region failover. */
public class RegionFailoverPerformanceTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(RegionFailoverPerformanceTest.class);

    private JobVertex source;
    private List<JobVertex> jobVertices;

    private ExecutionGraph executionGraph;
    private SchedulingTopology schedulingTopology;
    private RestartPipelinedRegionFailoverStrategy strategy;

    public void createRestartPipelinedRegionFailoverStrategy(JobConfiguration jobConfiguration)
            throws Exception {
        jobVertices = createDefaultJobVertices(jobConfiguration);
        source = jobVertices.get(0);
        executionGraph = createAndInitExecutionGraph(jobVertices, jobConfiguration);
        schedulingTopology = executionGraph.getSchedulingTopology();
        strategy = new RestartPipelinedRegionFailoverStrategy(schedulingTopology);
    }

    @Test
    public void testCalculateRegionToRestartInStreamingJobPerformance() throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.STREAMING;

        createRestartPipelinedRegionFailoverStrategy(jobConfiguration);

        TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        deployAllTasks(executionGraph, slotBuilder);

        switchAllVerticesToRunning(executionGraph);

        final long startTime = System.nanoTime();

        Set<ExecutionVertexID> tasks =
                strategy.getTasksNeedingRestart(
                        executionGraph.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
                        new Exception("For test."));

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));

        if (tasks.size() != jobConfiguration.getParallelism() * 2) {
            throw new RuntimeException(
                    String.format(
                            "Number of tasks to restart mismatch, expected %d, actual %d.",
                            jobConfiguration.getParallelism() * 2, tasks.size()));
        }
    }

    @Test
    public void testCalculateRegionToRestartInBatchJobPerformance() throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.BATCH;

        createRestartPipelinedRegionFailoverStrategy(jobConfiguration);

        final JobVertex source = jobVertices.get(0);
        final JobVertex sink = jobVertices.get(1);

        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        deployTasks(executionGraph, source.getID(), slotBuilder, true);

        transitionTaskStatus(executionGraph, source.getID(), ExecutionState.FINISHED);

        deployTasks(executionGraph, sink.getID(), slotBuilder, true);

        final long startTime = System.nanoTime();

        Set<ExecutionVertexID> tasks =
                strategy.getTasksNeedingRestart(
                        executionGraph.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
                        new Exception("For test."));

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        LOG.info(String.format("Duration of failover in the streaming task is : %d ms", duration));

        if (tasks.size() != jobConfiguration.getParallelism() + 1) {
            throw new RuntimeException(
                    String.format(
                            "Number of tasks to restart mismatch, expected %d, actual %d.",
                            jobConfiguration.getParallelism() + 1, tasks.size()));
        }
    }
}
