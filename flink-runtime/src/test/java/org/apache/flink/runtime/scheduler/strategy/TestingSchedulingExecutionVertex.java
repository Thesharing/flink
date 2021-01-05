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
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.topology.Group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A simple scheduling execution vertex for testing purposes. */
public class TestingSchedulingExecutionVertex implements SchedulingExecutionVertex {

    private final ExecutionVertexID executionVertexId;

    private final List<Group<IntermediateResultPartitionID>> consumerPartitions;

    private final Collection<TestingSchedulingResultPartition> producedPartitions;

    private final Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
            resultPartitionsById;

    private ExecutionState executionState;

    public TestingSchedulingExecutionVertex(
            JobVertexID jobVertexId,
            int subtaskIndex,
            List<Group<IntermediateResultPartitionID>> consumerPartitions,
            Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
                    resultPartitionsById,
            ExecutionState executionState) {

        this.executionVertexId = new ExecutionVertexID(jobVertexId, subtaskIndex);
        this.consumerPartitions = checkNotNull(consumerPartitions);
        this.producedPartitions = new ArrayList<>();
        this.resultPartitionsById = checkNotNull(resultPartitionsById);
        this.executionState = executionState;
    }

    @Override
    public ExecutionVertexID getId() {
        return executionVertexId;
    }

    @Override
    public ExecutionState getState() {
        return executionState;
    }

    public void setState(ExecutionState state) {
        this.executionState = state;
    }

    @Override
    public Iterable<TestingSchedulingResultPartition> getConsumedResults() {
        return consumerPartitions.stream()
                .map(Group::getItems)
                .flatMap(Collection::stream)
                .map(resultPartitionsById::get)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<TestingSchedulingResultPartition> getProducedResults() {
        return producedPartitions;
    }

    @Override
    public List<Group<IntermediateResultPartitionID>> getGroupedConsumedResults() {
        return consumerPartitions;
    }

    @Override
    public SchedulingResultPartition getResultPartition(IntermediateResultPartitionID id) {
        return resultPartitionsById.get(id);
    }

    void addConsumedPartition(Group<TestingSchedulingResultPartition> consumedPartitionGroup) {
        Group<IntermediateResultPartitionID> idGroup =
                new Group<>(new ArrayList<>(consumedPartitionGroup.getItems().size()));
        for (TestingSchedulingResultPartition partition : consumedPartitionGroup.getItems()) {
            idGroup.getItems().add(partition.getId());
            this.resultPartitionsById.putIfAbsent(partition.getId(), partition);
        }
        this.consumerPartitions.add(idGroup);
    }

    void addProducedPartition(TestingSchedulingResultPartition partition) {
        producedPartitions.add(partition);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static TestingSchedulingExecutionVertex withExecutionVertexID(
            JobVertexID jobVertexId, int subtaskIndex) {
        return newBuilder().withExecutionVertexID(jobVertexId, subtaskIndex).build();
    }

    /** Builder for {@link TestingSchedulingExecutionVertex}. */
    public static class Builder {
        private JobVertexID jobVertexId = new JobVertexID();
        private int subtaskIndex = 0;
        private List<Group<IntermediateResultPartitionID>> partitions = new ArrayList<>();
        private Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
                resultPartitionsById = new HashMap<>();
        private ExecutionState executionState = ExecutionState.CREATED;

        Builder withExecutionVertexID(JobVertexID jobVertexId, int subtaskIndex) {
            this.jobVertexId = jobVertexId;
            this.subtaskIndex = subtaskIndex;
            return this;
        }

        public Builder withConsumedPartitions(
                List<Group<IntermediateResultPartitionID>> partitions,
                Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
                        resultPartitionsById) {
            this.partitions = partitions;
            this.resultPartitionsById = resultPartitionsById;
            return this;
        }

        public Builder withConsumedPartitions(
                List<Group<TestingSchedulingResultPartition>> partitions) {
            this.partitions = new ArrayList<>();
            this.resultPartitionsById = new HashMap<>();

            for (Group<TestingSchedulingResultPartition> partitionGroup : partitions) {
                List<IntermediateResultPartitionID> ids =
                        new ArrayList<>(partitionGroup.getItems().size());
                this.partitions.add(new Group<>(ids));
                for (TestingSchedulingResultPartition partition : partitionGroup.getItems()) {
                    ids.add(partition.getId());
                    this.resultPartitionsById.putIfAbsent(partition.getId(), partition);
                }
            }
            return this;
        }

        public Builder withExecutionState(ExecutionState executionState) {
            this.executionState = executionState;
            return this;
        }

        public TestingSchedulingExecutionVertex build() {
            return new TestingSchedulingExecutionVertex(
                    jobVertexId, subtaskIndex, partitions, resultPartitionsById, executionState);
        }
    }
}
