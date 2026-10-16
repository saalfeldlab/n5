package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.kva.TrackingKeyValueRoot;

/**
 * An N5Writer that tracks the number of materialize calls performed by
 * its underlying key value access.
 */
public class TrackingN5Writer extends N5KeyValueWriter {

	public final TrackingKeyValueRoot tkvr;

	public TrackingN5Writer(final KeyValueRoot kvr) {

		super(new TrackingKeyValueRoot(kvr), new GsonBuilder(), false);
		this.tkvr = (TrackingKeyValueRoot) getKeyValueRoot();
	}

	public void resetNumMaterializeCalls() {
		tkvr.numMaterializeCalls = 0;
	}

	public int getNumMaterializeCalls() {
		return tkvr.numMaterializeCalls;
	}

	public void resetNumIsFileCalls() {
		tkvr.numIsFileCalls = 0;
	}

	public int getNumIsFileCalls() {
		return tkvr.numIsFileCalls;
	}

	public void resetTotalBytesRead() {
		tkvr.totalBytesRead = 0;
	}

	public long getTotalBytesRead() {
		return tkvr.totalBytesRead;
	}

	public void resetAllTracking() {
		tkvr.numMaterializeCalls = 0;
		tkvr.numIsFileCalls = 0;
		tkvr.totalBytesRead = 0;
	}
}
