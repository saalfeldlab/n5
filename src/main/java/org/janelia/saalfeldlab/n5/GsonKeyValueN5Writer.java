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

import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonSyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

/**
 * Default implementation of {@link N5Writer} with JSON attributes parsed with
 * {@link Gson}.
 */
public interface GsonKeyValueN5Writer extends GsonN5Writer, GsonKeyValueN5Reader {

	/**
	 * TODO This overrides the version even if incompatible, check
	 * if this is the desired behavior or if it is always overridden, e.g. as by
	 * the caching version. If this is true, delete this implementation.
	 *
	 * @param path to the group to write the version into
	 */
	default void setVersion(final String path) {

		if (!VERSION.equals(getVersion()))
			setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	static String initializeContainer(
			final KeyValueAccess keyValueAccess,
			final String basePath) throws N5IOException {

		final String normBasePath = keyValueAccess.normalize(basePath);
		keyValueAccess.createDirectories(normBasePath);
		return normBasePath;
	}

	@Override
	default void createGroup(final String path) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		getKeyValueAccess().createDirectories(absoluteGroupPath(normalPath));
	}

	/**
	 * Helper method that writes an attributes tree into the store
	 * <p>
	 * TODO This method is not part of the public API and should be protected
	 * in Java versions greater than 8
	 *
	 * @param normalGroupPath
	 *            to write the attributes to
	 * @param attributes
	 *            to write
	 * @throws N5Exception
	 *             if unable to write the attributes at {@code normalGroupPath}
	 */
	default void writeAttributes(
			final String normalGroupPath,
			final JsonElement attributes) throws N5Exception {

		final ReadData newAttributesReadData = ReadData.from(os -> {
			final OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8);
			GsonUtils.writeAttributes(writer, attributes, getGson());
		});

		try {
			getKeyValueAccess().write(absoluteAttributesPath(normalGroupPath), newAttributesReadData);
		} catch (UncheckedIOException | N5IOException e) {
			throw new N5Exception.N5IOException("Failed to write attributes into " + normalGroupPath, e);
		}
	}

	@Override
	default void setAttributes(
			final String path,
			final JsonElement attributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		if (!exists(normalPath))
			throw new N5IOException("" + normalPath + " is not a group or dataset.");

		writeAttributes(normalPath, attributes);
	}

	/**
	 * Helper method that reads the existing map of attributes, JSON encodes,
	 * inserts and overrides the provided attributes, and writes them back into
	 * the attributes store.
	 *
	 * TODO This method is not part of the public API and should be protected
	 * in Java greater than 8
	 *
	 * @param normalGroupPath
	 *            to write the attributes to
	 * @param attributes
	 *            to write
	 * @throws N5Exception
	 *             if unable to read or write the attributes at
	 *             {@code normalGroupPath}
	 */
	default void writeAttributes(
			final String normalGroupPath,
			final Map<String, ?> attributes) throws N5Exception {

		if (attributes != null && !attributes.isEmpty()) {
			JsonElement root = getAttributes(normalGroupPath);
			root = root != null && root.isJsonObject()
					? root.getAsJsonObject()
					: new JsonObject();
			root = GsonUtils.insertAttributes(root, attributes, getGson());
			writeAttributes(normalGroupPath, root);
		}
	}

	@Override
	default void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		if (!exists(normalPath))
			throw new N5IOException("" + normalPath + " is not a group or dataset.");

		writeAttributes(normalPath, attributes);
	}

	@Override
	default boolean removeAttribute(final String groupPath, final String attributePath) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(groupPath);
		final String absoluteNormalPath = getKeyValueAccess().compose(getURI(), normalPath);
		final String normalKey = N5URI.normalizeAttributePath(attributePath);

		if (!getKeyValueAccess().isDirectory(absoluteNormalPath))
			return false;

		if (attributePath.equals("/")) {
			setAttributes(normalPath, JsonNull.INSTANCE);
			return true;
		}

		final JsonElement attributes = getAttributes(normalPath);
		if (GsonUtils.removeAttribute(attributes, normalKey) != null) {
			setAttributes(normalPath, attributes);
			return true;
		}
		return false;
	}

	@Override
	default <T> T removeAttribute(final String pathName, final String key, final Class<T> cls) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		final String normalKey = N5URI.normalizeAttributePath(key);

		final JsonElement attributes = getAttributes(normalPath);
		final T obj;
		try {
			obj = GsonUtils.removeAttribute(attributes, normalKey, cls, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
		if (obj != null) {
			setAttributes(normalPath, attributes);
		}
		return obj;
	}

	@Override
	default boolean removeAttributes(final String pathName, final List<String> attributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		boolean removed = false;
		for (final String attribute : attributes) {
			final String normalKey = N5URI.normalizeAttributePath(attribute);
			removed |= removeAttribute(normalPath, normalKey);
		}
		return removed;
	}

	@Override
	default <T> void writeRegion(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> chunkSupplier,
			final boolean writeFully) throws N5Exception {
		DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeRegion(posKva, min, size, chunkSupplier, writeFully);
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
			final DataBlockSupplier<T> chunkSupplier,
			final boolean writeFully,
			final ExecutorService exec) throws N5Exception, InterruptedException, ExecutionException {
		DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeRegion(posKva, min, size, chunkSupplier, writeFully, exec);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write blocks into dataset " + datasetPath, e);
		}
	}

	@Override
	default <T> void writeChunks(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T>... chunks) throws N5Exception {

		DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeChunks(posKva, Arrays.asList(chunks));
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write chunks into dataset " + datasetPath, e);
		}
	}

	@Override
	default <T> void writeChunk(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> chunk) throws N5Exception {

		DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(path), convertedDatasetAttributes);
			convertedDatasetAttributes.<T> getDatasetAccess().writeChunk(posKva, chunk);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write chunk " + Arrays.toString(chunk.getGridPosition()) + " into dataset " + path,
					e);
		}
	}

	@Override
	default <T> void writeBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final int shardLevel = convertedDatasetAttributes.getNestedBlockGrid().numLevels() - 1;
		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(path), convertedDatasetAttributes);
			convertedDatasetAttributes.<T> getDatasetAccess().writeBlock(posKva, dataBlock, shardLevel);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write block " + Arrays.toString(dataBlock.getGridPosition()) + " into dataset " + path,
					e);
		}
	}

	@Override
	default boolean remove(final String path) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		final String groupPath = absoluteGroupPath(normalPath);
		if (getKeyValueAccess().isDirectory(groupPath))
			getKeyValueAccess().delete(groupPath);

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}

	@Override
	default boolean deleteBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(path), datasetAttributes);
		return posKva.remove(gridPosition);
	}

	@Override
	default boolean deleteChunk(
			final String path,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(path), datasetAttributes);
		return datasetAttributes.getDatasetAccess().deleteChunk(posKva, gridPosition);
	}

	@Override
	default boolean deleteChunks(
			final String path,
			final DatasetAttributes datasetAttributes,
			final List<long[]> gridPositions) throws N5Exception {

		final PositionValueAccess posKva = PositionValueAccess.fromKva(getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(path), datasetAttributes);
		return datasetAttributes.getDatasetAccess().deleteChunks(posKva, gridPositions);
	}
}