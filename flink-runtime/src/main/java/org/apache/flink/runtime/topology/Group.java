package org.apache.flink.runtime.topology;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.DistributionPattern;

import java.util.ArrayList;
import java.util.List;

/**
 * Grouped items like vertices and results.
 */
public class Group<E> {
	private final List<E> items;
	private final DistributionPattern distributionPattern;

	@VisibleForTesting
	public Group(List<E> items) {
		this(items, null);
	}

	public Group(DistributionPattern distributionPattern) {
		this(new ArrayList<>(), distributionPattern);
	}

	public Group(List<E> items, DistributionPattern distributionPattern) {
		this.items = items;
		this.distributionPattern = distributionPattern;
	}

	public List<E> getItems() {
		return items;
	}

	public DistributionPattern getDistributionPattern() {
		return distributionPattern;
	}
}
