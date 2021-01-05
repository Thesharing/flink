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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.topology.Group;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Utilities for building {@link EdgeManager}. */
public class EdgeManagerBuildUtil {

    public static void registerToExecutionEdgeManager(
            ExecutionVertex[] taskVertices,
            IntermediateResult ires,
            int inputNumber,
            DistributionPattern distributionPattern) {

        switch (distributionPattern) {
            case POINTWISE:
                connectPointwise(taskVertices, ires, inputNumber, distributionPattern);
                break;
            case ALL_TO_ALL:
                connectAllToAll(taskVertices, ires, inputNumber, distributionPattern);
                break;
            default:
                throw new RuntimeException("Unrecognized distribution pattern.");
        }
    }

    private static void connectAllToAll(
            ExecutionVertex[] taskVertices,
            IntermediateResult ires,
            int inputNumber,
            DistributionPattern distributionPattern) {

        Group<IntermediateResultPartitionID> consumedPartitions =
                new Group<>(
                        Arrays.stream(ires.getPartitions())
                                .map(IntermediateResultPartition::getPartitionId)
                                .collect(Collectors.toList()),
                        distributionPattern);
        for (ExecutionVertex ev : taskVertices) {
            ev.setConsumedPartitions(consumedPartitions, inputNumber);
        }

        Group<ExecutionVertexID> vertices =
                new Group<>(
                        Arrays.stream(taskVertices)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toList()),
                        distributionPattern);
        for (IntermediateResultPartition partition : ires.getPartitions()) {
            partition.setConsumers(vertices);
        }
    }

    private static void connectPointwise(
            ExecutionVertex[] taskVertices,
            IntermediateResult ires,
            int inputNumber,
            DistributionPattern distributionPattern) {

        final int parallelism = taskVertices[0].getParallelism();

        ArrayList<Group<IntermediateResultPartitionID>> partitions = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            partitions.add(new Group<>(distributionPattern));
        }
        for (int i = 0; i < ires.getPartitions().length; i++) {
            IntermediateResultPartition partition = ires.getPartitions()[i];
            Group<ExecutionVertexID> consumerExecutionVertices =
                    getConsumerExecutionVerticesPointwise(
                            taskVertices, ires, i, distributionPattern);
            for (ExecutionVertexID vertexID : consumerExecutionVertices.getItems()) {
                partitions
                        .get(vertexID.getSubtaskIndex())
                        .getItems()
                        .add(partition.getPartitionId());
            }

            partition.setConsumers(consumerExecutionVertices);
        }
        for (int i = 0; i < parallelism; i++) {
            ExecutionVertex ev = taskVertices[i];
            ev.setConsumedPartitions(partitions.get(i), inputNumber);
        }
    }

    private static Group<ExecutionVertexID> getConsumerExecutionVerticesPointwise(
            ExecutionVertex[] taskVertices,
            IntermediateResult ires,
            int partitionNumber,
            DistributionPattern distributionPattern) {

        final int sourceCount = ires.getProducer().getParallelism();
        final int targetCount = taskVertices[0].getParallelism();

        final Group<ExecutionVertexID> consumerVertices = new Group<>(distributionPattern);

        // simple case same number of sources as targets
        if (sourceCount == targetCount) {
            consumerVertices.getItems().add(taskVertices[partitionNumber].getID());
        } else if (sourceCount > targetCount) {
            int vertexSubtaskIndex;

            // check if the pattern is regular or irregular
            // we use int arithmetics for regular, and floating point with rounding for irregular
            if (sourceCount % targetCount == 0) {
                // same number of targets per source
                int factor = sourceCount / targetCount;
                vertexSubtaskIndex = partitionNumber / factor;
            } else {
                // different number of targets per source
                float factor = ((float) sourceCount) / targetCount;

                // Do mirror to generate the same edge mapping as in old Flink version
                int mirrorPartitionNumber = sourceCount - 1 - partitionNumber;
                int mirrorVertexSubTaskIndex = (int) (mirrorPartitionNumber / factor);
                vertexSubtaskIndex = targetCount - 1 - mirrorVertexSubTaskIndex;
            }
            consumerVertices.getItems().add(taskVertices[vertexSubtaskIndex].getID());
        } else {
            if (targetCount % sourceCount == 0) {
                // same number of targets per source
                int factor = targetCount / sourceCount;
                int startIndex = partitionNumber * factor;

                for (int i = 0; i < factor; i++) {
                    consumerVertices.getItems().add(taskVertices[startIndex + i].getID());
                }
            } else {
                float factor = ((float) targetCount) / sourceCount;

                // Do mirror to generate the same edge mapping as in old Flink version
                int mirrorPartitionNumber = sourceCount - 1 - partitionNumber;
                int start = (int) (mirrorPartitionNumber * factor);
                int end =
                        (mirrorPartitionNumber == sourceCount - 1)
                                ? targetCount
                                : (int) ((mirrorPartitionNumber + 1) * factor);

                for (int i = 0; i < end - start; i++) {
                    int mirrorVertexSubTaskIndex = start + i;
                    int vertexSubtaskIndex = targetCount - 1 - mirrorVertexSubTaskIndex;
                    consumerVertices.getItems().add(taskVertices[vertexSubtaskIndex].getID());
                }
            }
        }

        return consumerVertices;
    }
}
