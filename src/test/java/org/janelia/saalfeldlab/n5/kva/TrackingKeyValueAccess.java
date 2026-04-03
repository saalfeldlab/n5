package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import org.janelia.saalfeldlab.n5.readdata.prefetch.AggregatingPrefetchLazyRead;

public class TrackingKeyValueAccess extends DelegateKeyValueAccess {

    public int numMaterializeCalls = 0;
    public int numIsFileCalls = 0;
    public long totalBytesRead = 0;
    public boolean aggregate = false;

    public TrackingKeyValueAccess(final KeyValueAccess kva) {
        super(kva);
    }

    @Override
    public boolean isFile(String normalPath) {
        numIsFileCalls++;
        return kva.isFile(normalPath);
    }

    @Override
    public VolatileReadData createReadData(final String normalPath) {

        final VolatileReadData volatileReadData = kva.createReadData(normalPath);
        final TrackingLazyRead trackingLazyRead = new TrackingLazyRead(volatileReadData);
        LazyRead lazyRead = trackingLazyRead;
        if (aggregate)
            lazyRead = new AggregatingPrefetchLazyRead(trackingLazyRead);
        return VolatileReadData.from( lazyRead );
    }

    private class TrackingLazyRead implements LazyRead {

        private final VolatileReadData readData;

        TrackingLazyRead(final VolatileReadData readData) {
            this.readData = readData;
        }

        @Override
        public long size() throws N5Exception.N5IOException {

            return readData.requireLength();
        }

        @Override
        public ReadData materialize(final long offset, final long length) {

            numMaterializeCalls++;
            return readData.slice(offset, length).materialize();
        }

        @Override
        public void close() {

            readData.close();
        }
    }
}
