package org.janelia.saalfeldlab.n5.shardstuff;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * Idea is to wrap a KeyValueAccess and a dataset URI to be able to get/put values (ReadData) by {@code long[]} key
 */
public interface PositionValueAccess {

	/**
	 * @return ReadData for the given key or {@code null} if the key doesn't exist
	 */
	ReadData get(long[] key) throws N5Exception.N5IOException;

	void put(long[] key, ReadData data) throws N5Exception.N5IOException;

	void remove(long[] key) throws N5Exception.N5IOException;

	public static PositionValueAccess fromKva(
			final KeyValueAccess kva,
			final URI uri,
			final String normalPath) {

		return new KvaPositionValueAccess(kva, uri, normalPath);
	}

	class KvaPositionValueAccess implements PositionValueAccess {

		private final KeyValueAccess kva;
		private final URI uri;
		private final String normalPath;

		KvaPositionValueAccess(final KeyValueAccess kva,
				final URI uri,
				final String normalPath) {
			this.kva = kva;
			this.uri = uri;
			this.normalPath = normalPath;
		}

		// TODO this duplicates GsonKeyValueReader.absoluteDataBlockPath
		// is this where we want the logic?
		private String absolutePath(
				final long... gridPosition) {

			final String[] components = new String[gridPosition.length + 1];
			components[0] = normalPath;
			int i = 0;
			for (final long p : gridPosition)
				components[++i] = Long.toString(p);

			return kva.compose(uri, components);
		}

		@Override
		public ReadData get(long[] key) throws N5IOException {
			return kva.createReadData(absolutePath(key));
		}

		@Override
		public void put(long[] key, ReadData data) throws N5IOException {

			try ( final LockedChannel ch = kva.lockForWriting(absolutePath(key));
				  final OutputStream outputStream = ch.newOutputStream();) {
				data.writeTo(outputStream);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}

		@Override
		public void remove(long[] key) throws N5IOException {
			kva.delete( absolutePath(key));
		}

	}
}
