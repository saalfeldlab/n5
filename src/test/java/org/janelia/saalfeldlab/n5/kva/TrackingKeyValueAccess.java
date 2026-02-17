package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import org.janelia.saalfeldlab.n5.shard.ShardTest;

public class TrackingKeyValueAccess extends DelegateKeyValueAccess {

    public int numMaterializeCalls = 0;
    public int numIsFileCalls = 0;
    public long totalBytesRead = 0;

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
//			throw new N5NoSuchKeyException("Test No Such Key");
        return VolatileReadData.from(new TrackingVolatileReadData(kva.createReadData(normalPath)));
    }

    private class TrackingVolatileReadData implements LazyRead {

        private final VolatileReadData readData;

        TrackingVolatileReadData(final VolatileReadData readData) {
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
