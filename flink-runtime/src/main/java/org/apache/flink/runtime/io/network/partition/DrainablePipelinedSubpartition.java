package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which would drain the data buffer if view is failed.
 */
public class DrainablePipelinedSubpartition extends PipelinedSubpartition implements DrainableSubpartition {
	private static final Logger LOG = LoggerFactory.getLogger(DrainablePipelinedSubpartition.class);

	@GuardedBy("buffers")
	private BufferConsumer unfinishedBuffer;

	@GuardedBy("buffers")
	private boolean isDraining = false;

	DrainablePipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public boolean add(BufferConsumer bufferConsumer) {
		synchronized (buffers) {
			if (!isDraining) {
				return super.add(bufferConsumer);
			}

			clearFinishedBuffer();

			if (unfinishedBuffer != null) {
				unfinishedBuffer.build().recycleBuffer();
				checkState(unfinishedBuffer.isFinished());
				unfinishedBuffer.close();
				unfinishedBuffer = null;
			}

			bufferConsumer.build().recycleBuffer();
			if (bufferConsumer.isFinished()) {
				bufferConsumer.close();
			} else {
				unfinishedBuffer = bufferConsumer;
			}
			return true;
		}
	}

	@Override
	public void flush() {
		synchronized (buffers) {
			if (!isDraining) {
				super.flush();
			} else {
				if (unfinishedBuffer != null) {
					unfinishedBuffer.build().recycleBuffer();
					if (unfinishedBuffer.isFinished()) {
						unfinishedBuffer.close();
						unfinishedBuffer = null;
					}
				}
			}
		}
	}

	@Override
	public void finish() throws IOException {
		if (isDraining) {
			LOG.info("Draining subpartition {} is finished.", super.toString());
		}
		super.finish();
	}

	@Override
	public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		final boolean notifyDataAvailable;
		synchronized (buffers) {
			checkState(!isReleased());
			checkState(readView == null,
				"Subpartition %s of is being (or already has been) consumed, " +
					"but pipelined subpartitions can only be consumed once.", index, parent.getPartitionId());

			LOG.debug("{}: Creating read view for subpartition {} of partition {}.",
				parent.getOwningTaskName(), index, parent.getPartitionId());

			readView = new DrainablePipelinedSubpartitionView(this, availabilityListener);
			notifyDataAvailable = !buffers.isEmpty(); // TODO: Determine unfinishedBuffer?
		}
		if (!isDraining) {
			if (notifyDataAvailable) {
				notifyDataAvailable();
			}
		}

		return readView;
	}

	@Override
	public void release() {
		final PipelinedSubpartitionView view;
		synchronized (buffers) {
			unfinishedBuffer.close();
			view = readView;
			// Write down the isReleased status of the view before view.releaseAllResources
			boolean isReleased = view != null && !view.isReleased();
			super.release();
			if (isReleased) {
				// If there is still a view attached, it means this is an unexpected releasing, probably due to a failure
				// Notify view the releasing by trigger available
				// And this reading would cause an exception thrown
				// TODO: This still needs confirmation.
				// view.isReleased will be True, and the input gate will raise ProducerFailedException
				view.notifyDataAvailable();
			}
		}
	}

	void onSubpartitionConsumingFailure(Throwable throwable) {
		synchronized (buffers) {
			readView = null;
		}
		parent.onSubPartitionComsumingFailure(index, throwable);
	}

	@Override
	public int startDraining() {
		LOG.debug("{} start draining.", this);
		synchronized (buffers) {
			isDraining = true;
			return clearFinishedBuffer();
		}
	}

	@Override
	public void stopDraining() {
		LOG.debug("{} stop draining.", this);
		synchronized (buffers) {
			isDraining = false;

			if (readView != null) {
				if (!buffers.isEmpty()) {
					notifyDataAvailable();
				}
			}
		}
	}

	@Nullable
	@Override
	BufferAndBacklog pollBuffer() {
		if (isReleased()) {
			if (getFailureCause() != null) {
				throw new ProducerFailedException(getFailureCause());
			} else {
				throw new ProducerFailedException(new IllegalStateException(String.format("Result subpartition [%s] has been released", toString()))
			}
		}
	}

	@Override
	public boolean isDraining() {
		return isDraining;
	}

	@Override
	public String toString() {
		return String.format("DrainablePipelinedSubPartition %s [ isDraining %b ]", super.toString(), isDraining);
	}

	@VisibleForTesting
	BufferConsumer getUnfinishedBuffer() {
		return unfinishedBuffer;
	}

	private int clearFinishedBuffer() {
		int drainedBufferCount = 0;
		final Iterator<BufferConsumer> it = buffers.iterator();
		while (it.hasNext()) {
			final BufferConsumer buffer = it.next();
			buffer.build().recycleBuffer();
			if (buffer.isFinished()) {
				buffer.close();
				it.remove();
				drainedBufferCount++;
			}
		}
		return drainedBufferCount;
	}
}
