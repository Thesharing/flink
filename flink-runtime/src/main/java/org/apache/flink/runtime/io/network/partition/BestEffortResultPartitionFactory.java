package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Factory for {@link BestEffortResultPartition} to use in {@link BestEfforShuffleEnviroment}.
 */
public class BestEffortResultPartitionFactory extends ResultPartitionFactory {
	private static final Logger LOG = LoggerFactory.getLogger(BestEffortResultPartitionFactory.class);

	public BestEffortResultPartitionFactory(
		ResultPartitionManager partitionManager,
		FileChannelManager channelManager,
		BufferPoolFactory bufferPoolFactory,
		BoundedBlockingSubpartitionType blockingSubpartitionType,
		int networkBuffersPerChannel,
		int floatingNetworkBuffersPerGate,
		int networkBufferSize,
		boolean forcePartitionReleaseOnConsumption) {
		super(
			partitionManager,
			channelManager,
			bufferPoolFactory,
			blockingSubpartitionType,
			networkBuffersPerChannel,
			floatingNetworkBuffersPerGate,
			networkBufferSize,
			forcePartitionReleaseOnConsumption);
	}

	@VisibleForTesting
	@Override
	public ResultPartition create(
		String taskNameWithSubtaskAndId,
		ResultPartitionID id,
		ResultPartitionType type,
		int numberOfSubpartitions,
		int maxParallelism,
		FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {
		ResultSubpartition[] subpartitions = new ResultSubpartition[numberOfSubpartitions];

		ResultPartition partition = new BestEffortResultPartition(
			taskNameWithSubtaskAndId,
			id,
			type,
			subpartitions,
			maxParallelism,
			partitionManager,
			bufferPoolFactory
		);

		createSubpartitions(partition, type, blockingSubpartitionType, subpartitions);

		LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);

		return partition;
	}

	@Override
	void createSubpartitions(
		ResultPartition partition,
		ResultPartitionType type,
		BoundedBlockingSubpartitionType blockingSubpartitionType,
		ResultSubpartition[] subpartitions) {
		if (type.isBlocking()) {
			initializeBoundedBlockingPartitions(
				subpartitions,
				partition,
				blockingSubpartitionType,
				networkBufferSize,
				channelManager);
		} else {
			for (int i = 0; i < subpartitions.length; i++) {
				subpartitions[i] = new PipelinedSubpartition(i, partition);
			}
		}
	}
}
