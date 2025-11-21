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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
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
	 * Gets the {@link ReadData} for the DataBlock (or shard) at the given
	 * position in the block (or shard) grid.
	 * 
	 * @param key
	 *            The position of the data block or shard
	 * @return ReadData for the given key or {@code null} if the key doesn't
	 *         exist
	 * @throws N5Exception.N5IOException
	 *             if an error occurs while reading
	 */
	ReadData get(long[] key) throws N5Exception.N5IOException;

	void put(long[] key, ReadData data) throws N5Exception.N5IOException;

	boolean remove(long[] key) throws N5Exception.N5IOException;

	public static PositionValueAccess fromKva(
			final KeyValueAccess kva,
			final URI uri,
			final String normalPath,
			final DatasetAttributes attributes) {

		return new KvaPositionValueAccess(kva, uri, normalPath, attributes);
	}

	class KvaPositionValueAccess implements PositionValueAccess {

		private final KeyValueAccess kva;
		private final URI uri;
		private final String normalPath;
		private final DatasetAttributes attributes;

		KvaPositionValueAccess(final KeyValueAccess kva,
				final URI uri,
				final String normalPath,
				final DatasetAttributes attributes) {

			this.kva = kva;
			this.uri = uri;
			this.normalPath = normalPath;
			this.attributes = attributes;
		}

		/**
		 * Constructs the absolute path for a data block (or shard) at a given grid
		 * position.
		 *
		 * @param gridPosition
		 *            to the target data block
		 * @return the absolute path to the data block ad gridPosition
		 */
		protected String absolutePath( final long... gridPosition) {
			return kva.compose(uri, normalPath, attributes.relativeBlockPath(gridPosition));
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
