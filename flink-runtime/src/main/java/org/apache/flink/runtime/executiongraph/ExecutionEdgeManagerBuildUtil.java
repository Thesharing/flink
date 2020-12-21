package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utilities for building {@link ExecutionEdgeManager}.
 */
public class ExecutionEdgeManagerBuildUtil {

	public static void registerToExecutionEdgeManager(
		ExecutionVertex[] taskVertices,
		int parallelism,
		JobEdge jobEdge,
		IntermediateResult ires,
		int inputNumber) {

		switch (jobEdge.getDistributionPattern()) {
			case POINTWISE:
				connectPointwise(taskVertices, parallelism, jobEdge, ires, inputNumber);
				break;
			case ALL_TO_ALL:
				connectAllToAll(taskVertices, ires, inputNumber);
				break;
			default:
				throw new RuntimeException("Unrecognized distribution pattern.");
		}
	}

	private static void connectAllToAll(
		ExecutionVertex[] taskVertices,
		IntermediateResult ires,
		int inputNumber) {

		for (ExecutionVertex ev : taskVertices) {
			ev.setConsumedPartitions(ires.getPartitions(), inputNumber);
		}
		List<ExecutionVertex> vertices = new ArrayList<>(Arrays.asList(taskVertices));
		for (IntermediateResultPartition partition : ires.getPartitions()) {
			partition.setConsumers(vertices);
		}
	}

	private static void connectPointwise(
		ExecutionVertex[] taskVertices,
		int parallelism,
		JobEdge jobEdge,
		IntermediateResult ires,
		int inputNumber) {

		ArrayList<ArrayList<IntermediateResultPartition>> partitions = new ArrayList<>(parallelism);
		for (int i = 0; i < parallelism; i++) {
			partitions.add(new ArrayList<>());
		}
		for (int i = 0; i < ires.getPartitions().length; i++) {
			IntermediateResultPartition partition = ires.getPartitions()[i];
			List<ExecutionVertex> consumerExecutionVertices =
				getConsumerExecutionVerticesPointwise(taskVertices, jobEdge, i);
			for (ExecutionVertex vertex : consumerExecutionVertices) {
				partitions.get(vertex.getID().getSubtaskIndex()).add(partition);
			}

			partition.setConsumers(consumerExecutionVertices);
		}
		for (int i = 0; i < parallelism; i++) {
			ExecutionVertex ev = taskVertices[i];
			ev.setConsumedPartitions(
				partitions.get(i).toArray(new IntermediateResultPartition[]{}),
				inputNumber);
		}
	}

	private static List<ExecutionVertex> getConsumerExecutionVerticesPointwise(
		ExecutionVertex[] taskVertices,
		JobEdge jobEdge,
		int partitionNumber) {

		final IntermediateDataSet source = jobEdge.getSource();
		final JobVertex target = jobEdge.getTarget();

		final int sourceCount = source.getProducer().getParallelism();
		final int targetCount = target.getParallelism();

		final List<ExecutionVertex> consumerVertices = new ArrayList<>();

		// simple case same number of sources as targets
		if (sourceCount == targetCount) {
			consumerVertices.add(taskVertices[partitionNumber]);
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

			consumerVertices.add(taskVertices[vertexSubtaskIndex]);
		} else {
			if (targetCount % sourceCount == 0) {
				// same number of targets per source
				int factor = targetCount / sourceCount;
				int startIndex = partitionNumber * factor;

				consumerVertices.addAll(
					Arrays.asList(taskVertices).subList(startIndex, startIndex + factor));
			} else {
				float factor = ((float) targetCount) / sourceCount;

				// Do mirror to generate the same edge mapping as in old Flink version
				int mirrorPartitionNumber = sourceCount - 1 - partitionNumber;
				int start = (int) (mirrorPartitionNumber * factor);
				int end = (mirrorPartitionNumber == sourceCount - 1) ?
					targetCount :
					(int) ((mirrorPartitionNumber + 1) * factor);

				for (int i = 0; i < end - start; i++) {
					int mirrorVertexSubTaskIndex = start + i;
					int vertexSubtaskIndex = targetCount - 1 - mirrorVertexSubTaskIndex;
					consumerVertices.add(taskVertices[vertexSubtaskIndex]);
				}
			}
		}

		return consumerVertices;
	}
}
