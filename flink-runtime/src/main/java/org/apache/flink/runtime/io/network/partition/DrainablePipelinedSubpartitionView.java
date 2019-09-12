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

package org.apache.flink.runtime.io.network.partition;

import javax.annotation.Nullable;

/**
 * View over a drainable in-memory only subpartition. Inherited from {@link PipelinedSubpartitionView}.
 */
public class DrainablePipelinedSubpartitionView extends PipelinedSubpartitionView {

	DrainablePipelinedSubpartitionView(PipelinedSubpartition parent, BufferAvailabilityListener listener) {
		super(parent, listener);
	}

	@Override
	public void releaseAllResources() {
		releaseAllResources(null);
	}

	public void releaseAllResources(@Nullable Throwable throwable) {
		if (isReleased.compareAndSet(false, true)) {
			if (throwable == null) {
				parent.onConsumedSubpartition();
			} else {
				((DrainablePipelinedSubpartition) parent).onSubpartitionConsumingFailure(throwable);
			}
		}
	}

	@Override
	public String toString() {
		return String.format("DrainablePipelinedSubpartitionView(index: %d) of ResultPartition %s",
			parent.index,
			parent.parent.getPartitionId());
	}

	@Override
	public boolean isReleased() {
		return isReleased.get();
	}
}
