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
import java.net.URI;
import java.util.List;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface GsonKeyValueN5Reader extends GsonN5Reader, DelegateStore {

	RootedKeyValueAccess getRootedKeyValueAccess();

	@Override
	default URI getURI() {

		return getRootedKeyValueAccess().root();
	}

	default boolean groupExists(final String normalPath) {

		return getDelegateStore().store_isDirectory(N5GroupPath.of(normalPath));
	}

	@Override
	default boolean exists(final String pathName) {

		// NB: This method checks for existence of a group or dataset.
		//     For n5, every dataset must be a group, so checking for existence
		//     of a group is sufficient.
		return groupExists(pathName);
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5Exception {

		return getDatasetAttributes(pathName) != null;
	}

	/**
	 * Constructs the relative path for the attributes file of a group or dataset.
	 *
	 * @param normalPath
	 *            normalized group path without leading slash
	 * @return the absolute path to the attributes
	 */
	// TODO: not sure this method is worth being publicly exposed? Maybe better to just inline?
	default N5FilePath relativeAttributesPath(final String normalPath) {

		return N5GroupPath.of(normalPath).resolve(getAttributesKey()).asFile();
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

		return getDelegateStore().store_readAttributesJson(N5GroupPath.of(pathName), getAttributesKey());
	}

	@Override
	default String[] list(final String pathName) throws N5Exception {

		return getDelegateStore().store_listDirectories(N5GroupPath.of(pathName));
	}


	@Override
	default <T> DataBlock<T> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5GroupPath.of(pathName),
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
		final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5GroupPath.of(pathName), convertedDatasetAttributes);
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
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getRootedKeyValueAccess(), N5GroupPath.of(pathName),
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

		final N5Path path = N5GroupPath.of(pathName).resolve(datasetAttributes.relativeBlockPath(gridPosition));
		return getRootedKeyValueAccess().isFile(path);
	}






	// ------------------------------------------------------------------------
	//
	// -- DelegateStore --
	//

	default DelegateStore getDelegateStore() {
		return this;
	}


	// ------------------------------------------------------------------------
	//
	// -- DelegateStore : READ --
	//

	@Override
	default JsonElement store_readAttributesJson(
			final N5GroupPath group,
			final String filename) throws N5IOException {

		final N5FilePath attributesPath = group.resolve(filename).asFile();
		try (final VolatileReadData readData = getRootedKeyValueAccess().createReadData(attributesPath);) {
			// TODO: this (ReadData --> JsonElement) should go into GsonUtils?
			return GsonUtils.readAttributes(new InputStreamReader(readData.inputStream()), getGson());
		} catch (final N5NoSuchKeyException e) {
			return null;
		} catch (final UncheckedIOException | N5IOException e) {
			throw new N5IOException("Failed to read attributes from " + attributesPath, e);
		}
	}

	@Override
	default boolean store_isDirectory(final N5GroupPath group) {
//		TODO: throws N5IOException?

		return getRootedKeyValueAccess().isDirectory(group);
	}

	@Override
	default String[] store_listDirectories(final N5GroupPath group) throws N5IOException {

		return getRootedKeyValueAccess().listDirectories(group);
	}


	// ------------------------------------------------------------------------
	//
	// -- DelegateStore : WRITE --
	//

	// TODO: separate DelegateStore into Read and Write parts
	@Override
	default void store_writeAttributesJson(final N5GroupPath group, final String filename, final JsonElement attributes) throws N5IOException {
		throw new UnsupportedOperationException("TODO: separate DelegateStore into Read and Write parts");
	}

	// TODO: separate DelegateStore into Read and Write parts
	@Override
	default void store_removeDirectory(final N5GroupPath group) throws N5IOException {
		throw new UnsupportedOperationException("TODO: separate DelegateStore into Read and Write parts");
	}

	@Override
	default void store_createDirectories(N5GroupPath group) throws N5IOException {
		throw new UnsupportedOperationException("TODO: separate DelegateStore into Read and Write parts");
	}

	// ------------------------------------------------------------------------
	//
	// -- deprecated / semi-obsolete --
	//

	@Deprecated
	default KeyValueAccess getKeyValueAccess() {
		return getRootedKeyValueAccess().getKVA();
	}
}
