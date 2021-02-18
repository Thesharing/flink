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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.MemoryUsage;
import java.util.List;

import static org.apache.flink.test.runtime.performance.scheduler.MemoryUtil.convertToMiB;
import static org.apache.flink.test.runtime.performance.scheduler.MemoryUtil.getHeapMemory;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createDefaultJobVertices;
import static org.apache.flink.test.runtime.performance.scheduler.SchedulerPerformanceTestUtil.createJobGraph;

/** Performance test for topology building. */
public class TopologyBuildingPerformanceTest extends TestLogger {

    private static final Logger LOG =
            LoggerFactory.getLogger(TopologyBuildingPerformanceTest.class);

    @Test
    public void testBuildExecutionGraphInStreamingJobPerformance() throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.STREAMING;

        final List<JobVertex> jobVertices = createDefaultJobVertices(jobConfiguration);
        final JobGraph jobGraph = createJobGraph(jobConfiguration);

        final ExecutionGraph executionGraph =
                TestingExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();

        final long startTime = System.nanoTime();

        executionGraph.attachJobGraph(jobVertices);

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        LOG.info(String.format("Duration of building execution graph is : %d ms", duration));
    }

    @Test
    public void testBuildExecutionGraphInBatchJobPerformance() throws Exception {

        JobConfiguration jobConfiguration = JobConfiguration.BATCH;

        final List<JobVertex> jobVertices = createDefaultJobVertices(jobConfiguration);
        final JobGraph jobGraph = createJobGraph(jobConfiguration);

        final ExecutionGraph executionGraph =
                TestingExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();

        MemoryUsage startMemoryUsage = getHeapMemory();

        final long startTime = System.nanoTime();

        executionGraph.attachJobGraph(jobVertices);

        final long duration = (System.nanoTime() - startTime) / 1_000_000;

        MemoryUsage endMemoryUsage = getHeapMemory();

        LOG.info(String.format("Duration of building execution graph is : %d ms", duration));
        LOG.info(
                String.format(
                        "Used heap memory is: %f MiB",
                        convertToMiB(endMemoryUsage.getUsed() - startMemoryUsage.getUsed())));
    }
}
