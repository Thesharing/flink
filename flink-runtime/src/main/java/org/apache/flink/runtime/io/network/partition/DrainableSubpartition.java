package org.apache.flink.runtime.io.network.partition;

/**
 * A subpartition that supports draining data.
 */
public interface DrainableSubpartition {

	/**
	 * Begin draining.
	 * @return size of buffer released immediately
	 */
	int startDraining();

	/**
	 * Stop draining.
	 */
	void stopDraining();

	/**
	 * Return isDraining.
	 * @return the value of isDraining
	 */
	boolean isDraining();
}
