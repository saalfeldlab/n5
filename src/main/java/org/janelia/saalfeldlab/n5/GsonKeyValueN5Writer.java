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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;

/**
 * Default implementation of {@link N5Writer} with JSON attributes parsed with
 * {@link Gson}.
 */
public interface GsonKeyValueN5Writer extends GsonN5Writer, GsonKeyValueN5Reader {

	@Override
	default void createGroup(final String path) throws N5Exception {

		getN5Store().createGroup(N5DirectoryPath.of(path));
	}

	@Override
	default DatasetAttributes createDataset(final String datasetPath, final DatasetAttributes datasetAttributes) throws N5Exception {

		final DatasetAttributes attributes = getConvertedDatasetAttributes(datasetAttributes);
		getN5Store().createDataset(N5DirectoryPath.of(datasetPath), attributes);
		return attributes;
	}

	@Override
	default void setAttributes(final String path, final Map<String, ?> attributes) throws N5Exception {

		getN5Store().setAttributes(N5DirectoryPath.of(path), attributes);
	}

	@Override
	default boolean removeAttribute(final String path, final String attributePath) throws N5Exception {

		return getN5Store().removeAttribute(N5DirectoryPath.of(path), attributePath);
	}

	@Override
	default <T> T removeAttribute(final String path, final String attributePath, final Class<T> clazz) throws N5Exception {

		return getN5Store().removeAttribute(N5DirectoryPath.of(path), attributePath, clazz);
	}

	@Override
	default boolean remove(final String path) throws N5Exception {

		return getN5Store().remove(N5DirectoryPath.of(path));
	}

	@Override
	default <T> void writeRegion(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> dataBlocks,
			final boolean writeFully) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5DirectoryPath.of(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeRegion(posKva, min, size, dataBlocks, writeFully);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write blocks into dataset " + datasetPath, e);
		}
	}

	@Override
	default <T> void writeRegion(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> dataBlocks,
			final boolean writeFully,
			final ExecutorService exec) throws N5Exception, InterruptedException, ExecutionException {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5DirectoryPath.of(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeRegion(posKva, min, size, dataBlocks, writeFully, exec);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write blocks into dataset " + datasetPath, e);
		}
	}

	@Override
	default <T> void writeBlocks(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T>... dataBlocks) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5DirectoryPath.of(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeBlocks(posKva, Arrays.asList(dataBlocks));
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write blocks into dataset " + datasetPath, e);
		}
	}

	@Override
	default <T> void writeBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5DirectoryPath.of(path), convertedDatasetAttributes);
			convertedDatasetAttributes.<T> getDatasetAccess().writeBlock(posKva, dataBlock);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write block " + Arrays.toString(dataBlock.getGridPosition()) + " into dataset " + path,
					e);
		}
	}

	@Override
	default <T> void writeShard(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> shard) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final int shardLevel = convertedDatasetAttributes.getNestedBlockGrid().numLevels() - 1;
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5DirectoryPath.of(path), convertedDatasetAttributes);
			convertedDatasetAttributes.<T> getDatasetAccess().writeShard(posKva, shard, shardLevel);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write block " + Arrays.toString(shard.getGridPosition()) + " into dataset " + path,
					e);
		}
	}

	@Override
	default boolean deleteBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5DirectoryPath.of(path), datasetAttributes);
		return datasetAttributes.getDatasetAccess().deleteBlock(posKva, gridPosition);
	}

	@Override
	default boolean deleteBlocks(
			final String path,
			final DatasetAttributes datasetAttributes,
			final List<long[]> gridPositions) throws N5Exception {

		final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5DirectoryPath.of(path), datasetAttributes);
		return datasetAttributes.getDatasetAccess().deleteBlocks(posKva, gridPositions);
	}
}