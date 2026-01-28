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

import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.List;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface GsonKeyValueN5Reader extends GsonN5Reader {

	KeyValueAccess getKeyValueAccess();

	default boolean groupExists(final String normalPath) {

		return getKeyValueAccess().isDirectory(absoluteGroupPath(normalPath));
	}

	@Override
	default boolean exists(final String pathName) {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		return groupExists(normalPath) || datasetExists(normalPath);
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5Exception {

		// for n5, every dataset must be a group
		return getDatasetAttributes(pathName) != null;
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attribute
	 * @throws N5Exception if the attributes cannot be read
	 */
	@Override
	default JsonElement getAttributes(final String pathName) throws N5Exception {

		final String groupPath = N5URI.normalizeGroupPath(pathName);
		final String attributesPath = absoluteAttributesPath(groupPath);

		try (final VolatileReadData readData = getKeyValueAccess().createReadData(attributesPath);) {
			if (readData == null) {
				return null;
			}
			return GsonUtils.readAttributes(new InputStreamReader(readData.inputStream()), getGson());
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final UncheckedIOException | N5IOException e) {
			throw new N5IOException("Failed to read attributes from dataset " + pathName, e);
		}
	}

	@Override
	default <T> DataBlock<T> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(pathName),
					convertedDatasetAttributes);
			return convertedDatasetAttributes.<T> getDatasetAccess().readBlock(posKva, gridPosition);

		} catch (N5Exception.N5NoSuchKeyException e) {
			return null;
		}
	}

	@Override
	default <T> List<DataBlock<T>> readBlocks(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final List<long[]> blockPositions) throws N5Exception {

		DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(pathName), convertedDatasetAttributes);
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
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(pathName),
					convertedDatasetAttributes);
			return convertedDatasetAttributes.<T> getDatasetAccess().readShard(posKva, gridPosition, shardLevel);

		} catch (N5Exception.N5NoSuchKeyException e) {
			return null;
		}
	}

	@Override
	default String[] list(final String pathName) throws N5Exception {

		return getKeyValueAccess().listDirectories(absoluteGroupPath(pathName));
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the group or
	 * dataset.
	 *
	 * @param normalGroupPath
	 *            normalized group path without leading slash
	 * @return the absolute path to the group
	 */
	default String absoluteGroupPath(final String normalGroupPath) {

		return getKeyValueAccess().compose(getURI(), normalGroupPath);
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the attributes
	 * file of a group or dataset.
	 *
	 * @param normalPath
	 *            normalized group path without leading slash
	 * @return the absolute path to the attributes
	 */
	default String absoluteAttributesPath(final String normalPath) {

		return getKeyValueAccess().compose(getURI(), normalPath, getAttributesKey());
	}

	@Override
	default boolean shardExists(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		final String blockPath = getKeyValueAccess().compose(getURI(), normalPath,
				datasetAttributes.relativeBlockPath(gridPosition));
		return getKeyValueAccess().isFile(blockPath);
	}
}
