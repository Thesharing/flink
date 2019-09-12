package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.function.FunctionWithException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A result partition for Best Effort Failover Strategy.
 */
public class BestEffortResultPartition extends ResultPartition {
	enum ConsumerEvent {
		CONSUMER_FAILED,
		CONSUMER_ATTACHED
	}

	private final ArrayDeque<ConsumerEvent>[] consumerEvents;
	private final AtomicBoolean[] consumerEventTriggered;

	public BestEffortResultPartition(
		String owningTaskName,
		ResultPartitionID partitionID,
		ResultPartitionType partitionType,
		ResultSubpartition[] subpartitions,
		int numTargetKeyGroups,
		ResultPartitionManager partitionManager,
		FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory
	) {
		super(owningTaskName, partitionID, partitionType, subpartitions, numTargetKeyGroups, partitionManager, bufferPoolFactory);
		consumerEvents = new ArrayDeque[getNumberOfSubpartitions()];
		consumerEventTriggered = new AtomicBoolean[getNumberOfSubpartitions()];

		for (int i = 0; i < getNumberOfSubpartitions(); i++) {
			consumerEvents[i] = new ArrayDeque<>();
			consumerEventTriggered[i] = new AtomicBoolean(false);
		}
	}

	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		synchronized (consumerEvents[index]) {
			consumerEvents[index].add(ConsumerEvent.CONSUMER_ATTACHED);
			consumerEventTriggered[index].set(true);
		}
		return super.createSubpartitionView(index, availabilityListener);
	}

	@Override
	public void releaseMemory(int toRelease) throws IOException {
		checkArgument(toRelease > 0);

		for (int i = 0; i < getNumberOfSubpartitions(); i++) {
			if (consumerEventTriggered[i].get()) {
				synchronized (consumerEvents[i]) {
					checkState(!consumerEvents[i].isEmpty());
					// If it's notified by consumer failure event, the first event in queue must be consumer failure event.
					if (consumerEvents[i].peek() == ConsumerEvent.CONSUMER_FAILED) {
						consumerEvents[i].poll();
						// Start draining to avoid back-pressure
						// Draining would release buffers for next request
						toRelease -= drainSubpartition(i);
						if (consumerEvents[i].isEmpty()) {
							consumerEventTriggered[i].set(false);
						}
					}
				}
			}
			if (toRelease <= 0) {
				break;
			}
		}
	}

	/**
	 * Notifies when a subpartition view is failed.
	 *
	 * @param subpartitionIndex index of subpartition
	 * @param throwable         throwable failure cause
	 */
	void onSubpartitionConsumingFailure(int subpartitionIndex, Throwable throwable) {
		LOG.debug("Consumer of subpartition {} failed {}.", subpartitionIndex, throwable);
		synchronized (consumerEvents[subpartitionIndex]) {
			consumerEvents[subpartitionIndex].add(ConsumerEvent.CONSUMER_FAILED);
			consumerEventTriggered[subpartitionIndex].set(true);
		}
	}

	@Override
	public boolean addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
		// TODO: Since no subpartitionIndex provided in getBufferBuilder, we reattach the consumer here
		if (consumerEventTriggered[subpartitionIndex].get()) {
			processConsumerEvents(subpartitionIndex);
		}
		return super.addBufferConsumer(bufferConsumer, subpartitionIndex);
	}

	private int drainSubpartition(int subpartitionIndex) {
		int bufferReleased = 0;
		final ResultSubpartition resultSubpartition = subpartitions[subpartitionIndex];
		// TODO: Type cast is necessary?
		if (resultSubpartition instanceof DrainablePipelinedSubpartition) {
			if (!((DrainablePipelinedSubpartition) resultSubpartition).isDraining()) {
				bufferReleased += ((DrainablePipelinedSubpartition) resultSubpartition).startDraining();
			}
		}
		return bufferReleased;
	}

	private void finishDrainingSubpartition(int subpartitionIndex) {
		final ResultSubpartition resultSubpartition = subpartitions[subpartitionIndex];
		if (resultSubpartition instanceof DrainablePipelinedSubpartition) {
			if (((DrainablePipelinedSubpartition) resultSubpartition).isDraining()) {
				((DrainablePipelinedSubpartition) resultSubpartition).stopDraining();
			}
		}
	}

	private void processConsumerEvents(int subpartitionIndex) {
		synchronized (consumerEvents[subpartitionIndex]) {
			ConsumerEvent event;
			while ((event = consumerEvents[subpartitionIndex].poll()) != null) {
				switch (event) {
					case CONSUMER_FAILED:
						drainSubpartition(subpartitionIndex);
						break;
					case CONSUMER_ATTACHED:
						finishDrainingSubpartition(subpartitionIndex);
						// TODO: Cannot `tryFinishCurrentBufferBuilder` here, so the metrics may be inaccurate
				}
			}
		}
	}

	@VisibleForTesting
	AtomicBoolean[] getConsumerEventTriggered() {
		return consumerEventTriggered;
	}

	@VisibleForTesting
	ArrayDeque<ConsumerEvent>[] getConsumerEvents() {
		return consumerEvents;
	}
}
