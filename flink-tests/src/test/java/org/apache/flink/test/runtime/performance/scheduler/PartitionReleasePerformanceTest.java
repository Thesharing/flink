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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createAndInitExecutionGraph;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createDefaultJobVertices;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.deployTasks;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.transitionTaskStatus;

/** Performance test for partition release. */
public class PartitionReleasePerformanceTest extends TestLogger {

    private static final Logger LOG =
            LoggerFactory.getLogger(PartitionReleasePerformanceTest.class);

    @Test
    public void testReleaseUpstreamPartitionPerformance() throws Exception {

        final JobConfiguration jobConfiguration = JobConfiguration.BATCH;

        final List<JobVertex> jobVertices = createDefaultJobVertices(jobConfiguration);

        final ExecutionGraph executionGraph =
                createAndInitExecutionGraph(jobVertices, jobConfiguration);

        final JobVertex source = jobVertices.get(0);
        final JobVertex sink = jobVertices.get(1);

        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        deployTasks(executionGraph, source.getID(), slotBuilder, true);

        transitionTaskStatus(executionGraph, source.getID(), ExecutionState.FINISHED);

        deployTasks(executionGraph, sink.getID(), slotBuilder, true);

        final long startTime = System.nanoTime();

        transitionTaskStatus(executionGraph, sink.getID(), ExecutionState.FINISHED);

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        LOG.info(String.format("Duration of releasing upstream partition is : %d ms", duration));
    }
}
