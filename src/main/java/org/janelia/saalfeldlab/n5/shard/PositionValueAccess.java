/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.shard;

import java.net.URI;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

/**
 * Wrap a KeyValueAccess and a dataset URI to be able to get/set values (ReadData) by {@code long[]} key
 * indicating the position of a block or shard.
 */
public interface PositionValueAccess {

	/**
	 * Gets the {@link VolatileReadData} for the DataBlock (or shard) at the
	 * given position in the block (or shard) grid.
	 * <p>
	 * If the requested key does not exist, either {@code null} is returned or a
	 * lazy {@code VolatileReadData} that will throw {@code N5NoSuchKeyException}
	 * when trying to materialize.
	 *
	 * @param key
	 *            The position of the data block or shard
	 * @return ReadData for the given key or {@code null} if the key doesn't
	 *         exist
	 * @throws N5Exception.N5IOException
	 *             if an error occurs while reading
	 */
	VolatileReadData get(long[] key) throws N5Exception.N5IOException;

	/**
	 * Write the {@code data} for a DataBlock (or shard) to the given position
	 * in the block grid.
	 *
	 * @param key
	 * 		The grid position of the DataBlock (or shard) to write
	 * @param data
	 * 		The data to write
	 *
	 * @throws N5Exception.N5IOException
	 * 		if an error occurs while writing
	 */
	void set(long[] key, ReadData data) throws N5Exception.N5IOException;

	boolean exists(long[] key) throws N5Exception.N5IOException;

	boolean remove(long[] key) throws N5Exception.N5IOException;

	static PositionValueAccess fromKva(
			final RootedKeyValueAccess kva,
			final N5DirectoryPath normalPath,
			final DatasetAttributes attributes) {

		return new RootedKvaPositionValueAccess(kva, normalPath, attributes);
	}

	class RootedKvaPositionValueAccess implements PositionValueAccess {

		private final RootedKeyValueAccess kva;
		private final N5DirectoryPath normalPath;
		private final DatasetAttributes attributes;

		RootedKvaPositionValueAccess(final RootedKeyValueAccess kva,
				final N5DirectoryPath normalPath,
				final DatasetAttributes attributes) {

			this.kva = kva;
			this.normalPath = normalPath;
			this.attributes = attributes;
		}

		/**
		 * Constructs the relative path for a data block (or shard) at a given
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
				return kva.createReadData(relativePath(key));
			} catch (N5Exception.N5NoSuchKeyException e) {
				return null;
			}
		}

		@Override
		public boolean exists(final long[] key) throws N5IOException {
			return kva.isFile(relativePath(key));
		}

		@Override
		public void set(final long[] key, final ReadData data) throws N5IOException {
			if (data == null) {
				remove(key);
			} else {
				kva.write(relativePath(key), data);
			}
		}

		@Override
		public boolean remove(final long[] gridPosition) throws N5IOException {
			final N5FilePath key = relativePath(gridPosition);
			if (!kva.isFile(key))
				return false;

			kva.delete(key);
			return true;
		}
	}

}
