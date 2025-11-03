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

	boolean remove(long[] key) throws N5Exception.N5IOException;

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

		/**
		 * Constructs the path for a shard or data block at a given
		 * grid position.
		 * <br>
		 * If the gridPosition passed in refers to shard position in a sharded
		 * dataset, this will return the path to the shard key.
		 *
		 * @param normalPath
		 *            normalized dataset path
		 * @param gridPosition
		 *            to the target data block
		 * @return the absolute path to the data block ad gridPosition
		 */
		protected String absolutePath( final long... gridPosition) {
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
		public boolean remove(long[] gridPosition) throws N5IOException {

			final String key = absolutePath(gridPosition);
			if (!kva.isFile(key))
				return false;

			kva.delete(key);
			return true;
		}

	}
}
