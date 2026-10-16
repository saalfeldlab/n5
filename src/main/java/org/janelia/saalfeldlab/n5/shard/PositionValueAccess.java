package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

/**
 * Wrap a KeyValueAccess and a dataset URI to be able to get/set values (ReadData) by {@code long[]} key
 * indicating the position of a top-level DataBlock (a top-level shard for sharded datasets,
 * a chunk for non-sharded datasets).
 */
public interface PositionValueAccess {

	/**
	 * Gets the {@link VolatileReadData} for the DataBlock at the given position
	 * in the block grid.
	 * <p>
	 * If the requested key does not exist, either {@code null} is returned or a
	 * lazy {@code VolatileReadData} that will throw {@code N5NoSuchKeyException}
	 * when trying to materialize.
	 *
	 * @param key
	 *            The position of the block
	 * @return ReadData for the given key or {@code null} if the key doesn't exist
	 * @throws N5Exception.N5IOException
	 *             if an error occurs while reading
	 */
	VolatileReadData get(long[] key) throws N5Exception.N5IOException;

	/**
	 * Write the {@code data} for a DataBlock to the given position in the block
	 * grid.
	 *
	 * @param key
	 * 		The grid position of the DataBlock to write
	 * @param data
	 * 		The data to write
	 *
	 * @throws N5Exception.N5IOException
	 * 		if an error occurs while writing
	 */
	void set(long[] key, ReadData data) throws N5Exception.N5IOException;

	boolean exists(long[] key) throws N5Exception.N5IOException;

	boolean remove(long[] key) throws N5Exception.N5IOException;

	static PositionValueAccess fromKeyValueRoot(
			final KeyValueRoot kvr,
			final N5DirectoryPath normalPath,
			final DatasetAttributes attributes) {

		return new KvrPositionValueAccess(kvr, normalPath, attributes);
	}

	class KvrPositionValueAccess implements PositionValueAccess {

		private final KeyValueRoot kvr;
		private final N5DirectoryPath normalPath;
		private final DatasetAttributes attributes;

		KvrPositionValueAccess(final KeyValueRoot kvr,
				final N5DirectoryPath normalPath,
				final DatasetAttributes attributes) {

			this.kvr = kvr;
			this.normalPath = normalPath;
			this.attributes = attributes;
		}

		/**
		 * Constructs the relative path for a DataBlock at a given
		 * grid position.
		 *
		 * @param gridPosition
		 * 		grid coordinates of the data block
		 *
		 * @return the path (relative to container root) of the data block at gridPosition
		 */
		private N5FilePath relativePath(final long... gridPosition) {
			return normalPath.resolve(attributes.relativeBlockPath(gridPosition)).asFile();
		}

		@Override
		public VolatileReadData get(final long[] key) throws N5IOException {
			try {
				return kvr.createReadData(relativePath(key));
			} catch (N5Exception.N5NoSuchKeyException e) {
				return null;
			}
		}

		@Override
		public boolean exists(final long[] key) throws N5IOException {
			return kvr.isFile(relativePath(key));
		}

		@Override
		public void set(final long[] key, final ReadData data) throws N5IOException {
			if (data == null) {
				remove(key);
			} else {
				kvr.write(relativePath(key), data);
			}
		}

		@Override
		public boolean remove(final long[] gridPosition) throws N5IOException {
			final N5FilePath key = relativePath(gridPosition);
			if (!kvr.isFile(key))
				return false;

			kvr.delete(key);
			return true;
		}
	}

}
