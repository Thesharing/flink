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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.MemoryUsage;
import java.util.List;

import static org.apache.flink.test.runtime.performance.scheduler.MemoryUtil.convertToMiB;
import static org.apache.flink.test.runtime.performance.scheduler.MemoryUtil.getHeapMemory;

/** Performance test for topology building. */
public class TopologyBuildingPerformanceTest extends TestLogger {

    private static final Logger LOG =
            LoggerFactory.getLogger(TopologyBuildingPerformanceTest.class);

    public static final int PARALLELISM = SchedulerPerformanceTestUtil.PARALLELISM;

    @Test
    public void testBuildExecutionGraphInStreamingJobPerformance() throws Exception {
        final List<JobVertex> jobVertices =
                SchedulerPerformanceTestUtil.createDefaultJobVertices(
                        PARALLELISM, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        final JobGraph jobGraph =
                SchedulerPerformanceTestUtil.createJobGraph(
                        jobVertices, ScheduleMode.EAGER, ExecutionMode.PIPELINED);

        final ExecutionGraph eg = SchedulerPerformanceTestUtil.createExecutionGraph(jobGraph);

        final long startTime = System.nanoTime();

        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        LOG.info(String.format("Duration of building execution graph is : %d ms", duration));
    }

    @Test
    public void testBuildExecutionGraphInBatchJobPerformance() throws Exception {
        final List<JobVertex> jobVertices =
                SchedulerPerformanceTestUtil.createDefaultJobVertices(
                        PARALLELISM, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        final JobGraph jobGraph =
                SchedulerPerformanceTestUtil.createJobGraph(
                        jobVertices, ScheduleMode.LAZY_FROM_SOURCES, ExecutionMode.BATCH);

        final ExecutionGraph eg = SchedulerPerformanceTestUtil.createExecutionGraph(jobGraph);

        MemoryUsage startMemoryUsage = getHeapMemory();

        final long startTime = System.nanoTime();

        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        MemoryUsage endMemoryUsage = getHeapMemory();

        LOG.info(String.format("Duration of building execution graph is : %d ms", duration));
        LOG.info(
                String.format(
                        "Used heap memory is: %f MiB",
                        convertToMiB(endMemoryUsage.getUsed() - startMemoryUsage.getUsed())));
    }
}
