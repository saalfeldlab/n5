package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Path;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class TrackingKeyValueRoot extends DelegateKeyValueRoot {

	public int numMaterializeCalls = 0;
	public int numIsFileCalls = 0;
	public long totalBytesRead = 0;

	public TrackingKeyValueRoot(final KeyValueRoot kvr) {
		super(kvr);
	}

	@Override
	public boolean isFile(final N5Path normalPath) {
		numIsFileCalls++;
		return kvr.isFile(normalPath);
	}

	@Override
	public VolatileReadData createReadData(final N5FilePath normalPath) {
		return VolatileReadData.from(new TrackingVolatileReadData(kvr.createReadData(normalPath)));
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
