package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.function.Function;

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
			final String normalPath,
			final Function<long[], String> blockPositionToPath) {

		return new KvaPositionValueAccess(kva, uri, normalPath, blockPositionToPath);
	}

	class KvaPositionValueAccess implements PositionValueAccess {

		private final KeyValueAccess kva;
		private final URI uri;
		private final String normalPath;
		private final Function<long[],String> blockPositionToPath;

		KvaPositionValueAccess(final KeyValueAccess kva,
				final URI uri,
				final String normalPath,
				final Function<long[], String> blockPositionToPath) {

			this.kva = kva;
			this.uri = uri;
			this.normalPath = normalPath;
			this.blockPositionToPath = blockPositionToPath;
		}

		// TODO this duplicates GsonKeyValueReader.absoluteDataBlockPath
		// is this where we want the logic?
		private String absolutePath( final long... gridPosition) {
			return kva.compose(uri, normalPath, blockPositionToPath.apply(gridPosition));
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
