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
package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface GsonKeyValueN5Reader extends GsonN5Reader {

	RootedKeyValueAccess getKeyValueRoot();

	N5Store getN5Store();

	@Override
	default URI getURI() {

		return getKeyValueRoot().root();
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Type type) throws N5Exception {

		return getN5Store().getAttribute(N5DirectoryPath.of(pathName), key, type);
	}

	@Override
	default DatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		return getN5Store().getDatasetAttributes(N5DirectoryPath.of(pathName));
	}

	@Override
	default Map<String, Class<?>> listAttributes(final String pathName) throws N5Exception {

		return getN5Store().listAttributes(N5DirectoryPath.of(pathName));
	}

	default boolean groupExists(final String pathName) {

		return getN5Store().groupExists(N5DirectoryPath.of(pathName));
	}

	@Override
	default boolean exists(final String pathName) {

		return groupExists(pathName) || datasetExists(pathName);
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5Exception {

		return getN5Store().datasetExists(N5DirectoryPath.of(pathName));
	}

	@Override
	default String[] list(final String pathName) throws N5Exception {

		return getN5Store().list(N5DirectoryPath.of(pathName));
	}


	@Override
	default <T> DataBlock<T> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueRoot(), N5DirectoryPath.of(pathName),
					convertedDatasetAttributes);
			return convertedDatasetAttributes.<T> getDatasetAccess().readBlock(posKva, gridPosition);

		} catch (N5NoSuchKeyException e) {
			return null;
		}
	}

	@Override
	default <T> List<DataBlock<T>> readBlocks(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final List<long[]> blockPositions) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueRoot(), N5DirectoryPath.of(pathName), convertedDatasetAttributes);
		return convertedDatasetAttributes.<T> getDatasetAccess().readBlocks(posKva, blockPositions);
	}

	@Override
	default <T> DataBlock<T> readShard(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final int shardLevel = convertedDatasetAttributes.getNestedBlockGrid().numLevels() - 1;
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueRoot(), N5DirectoryPath.of(pathName),
					convertedDatasetAttributes);
			return convertedDatasetAttributes.<T> getDatasetAccess().readShard(posKva, gridPosition, shardLevel);

		} catch (N5NoSuchKeyException e) {
			return null;
		}
	}

	@Override
	default boolean shardExists(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final N5Path path = N5DirectoryPath.of(pathName).resolve(datasetAttributes.relativeBlockPath(gridPosition));
		return getKeyValueRoot().isFile(path);
	}


	// ------------------------------------------------------------------------
	//
	// -- deprecated / semi-obsolete --
	//

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attribute
	 * @throws N5Exception if the attributes cannot be read
	 */
	@Override
	default JsonElement getAttributes(final String pathName) throws N5Exception { // TODO: deprecate?

		return getN5Store().getAttributes(N5DirectoryPath.of(pathName));
	}

	@Deprecated
	default KeyValueAccess getKeyValueAccess() {
		return getKeyValueRoot().getKVA();
	}
}
