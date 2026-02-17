package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.kva.TrackingKeyValueAccess;

/**
 * An N5Writer that tracks the number of materialize calls performed by
 * its underlying key value access.
 */
public class TrackingN5Writer extends N5KeyValueWriter {

    public final TrackingKeyValueAccess tkva;

    public TrackingN5Writer(String basePath, KeyValueAccess kva) {

        super(new TrackingKeyValueAccess(kva), basePath, new GsonBuilder(), false);
        this.tkva = (TrackingKeyValueAccess) getKeyValueAccess();
    }

    public void resetNumMaterializeCalls() {
        tkva.numMaterializeCalls = 0;
    }

    public int getNumMaterializeCalls() {
        return tkva.numMaterializeCalls;
    }

    public void resetNumIsFileCalls() {
        tkva.numIsFileCalls = 0;
    }

    public int getNumIsFileCalls() {
        return tkva.numIsFileCalls;
    }

    public void resetTotalBytesRead() {
        tkva.totalBytesRead = 0;
    }

    public long getTotalBytesRead() {
        return tkva.totalBytesRead;
    }

    public void resetAllTracking() {
        tkva.numMaterializeCalls = 0;
        tkva.numIsFileCalls = 0;
        tkva.totalBytesRead = 0;
    }
}
