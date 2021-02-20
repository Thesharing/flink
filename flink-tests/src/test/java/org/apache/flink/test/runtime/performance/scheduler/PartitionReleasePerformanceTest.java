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
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Performance test for partition release. */
public class PartitionReleasePerformanceTest extends TestLogger {

    private static final Logger LOG =
            LoggerFactory.getLogger(PartitionReleasePerformanceTest.class);

    public static final int PARALLELISM = SchedulerPerformanceTestUtil.PARALLELISM;

    @Test
    public void testReleaseUpstreamPartitionPerformance() throws Exception {
        final List<JobVertex> jobVertices =
                SchedulerPerformanceTestUtil.createDefaultJobVertices(
                        PARALLELISM, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        final JobGraph jobGraph =
                SchedulerPerformanceTestUtil.createJobGraph(
                        jobVertices, ScheduleMode.LAZY_FROM_SOURCES, ExecutionMode.BATCH);

        final ExecutionGraph eg = SchedulerPerformanceTestUtil.createExecutionGraph(jobGraph);

        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

        TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        JobVertex source = jobVertices.get(0);
        SchedulerPerformanceTestUtil.deployTasks(eg, source.getID(), slotBuilder, false);
        SchedulerPerformanceTestUtil.transitionTaskStatus(
                eg, source.getID(), ExecutionState.FINISHED);

        JobVertex sink = jobVertices.get(1);
        SchedulerPerformanceTestUtil.deployTasks(eg, sink.getID(), slotBuilder, false);

        final long startTime = System.nanoTime();

        SchedulerPerformanceTestUtil.transitionTaskStatus(
                eg, sink.getID(), ExecutionState.FINISHED);

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        LOG.info(String.format("Duration of releasing upstream partition is : %d ms", duration));
    }
}
