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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.gson.JsonSyntaxException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.shardstuff.PositionValueAccess;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.janelia.saalfeldlab.n5.shard.InMemoryShard;
import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.util.Position;

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
	 *
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

		try (final LockedChannel lock = getKeyValueAccess().lockForWriting(absoluteAttributesPath(normalGroupPath))) {
			GsonUtils.writeAttributes(lock.newWriter(), attributes, getGson());
		} catch (final IOException | UncheckedIOException | N5IOException e) {
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
			writeAttributes(normalPath, attributes);
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
	default <T> void writeBlocks(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T>... dataBlocks) throws N5Exception {

		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(
					getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(datasetPath));
			datasetAttributes.<T>getDatasetAccess().writeBlocks(posKva, Arrays.asList(dataBlocks));
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

		try {
			final PositionValueAccess posKva = PositionValueAccess.fromKva(
					getKeyValueAccess(), getURI(), N5URI.normalizeGroupPath(path));
			datasetAttributes.<T>getDatasetAccess().writeBlock(posKva, dataBlock);
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
			final Shard<T> shard) throws N5Exception {

		// TODO

//		final String shardPath = absoluteDataBlockPath(N5URI.normalizeGroupPath(path), shard.getGridPosition());
//		try (
//				final LockedChannel channel = getKeyValueAccess().lockForWriting(shardPath);
//				final OutputStream shardOut = channel.newOutputStream()
//		) {
//			shard.createReadData().writeTo(shardOut);
//		} catch (final IOException | UncheckedIOException e) {
//			throw new N5IOException(
//					"Failed to write shard " + Arrays.toString(shard.getGridPosition()) + " into dataset " + path, e);
//		}
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

	/**
	 * Delete a shard at the specified position.
	 *
	 * @param path
	 *            the dataset path
	 * @param shardPosition
	 *            the position of the shard to delete
	 * @return true if the shard existed was successfully deleted. 
	 * @throws N5Exception
	 *             if an error occurs during deletion
	 */
	default boolean deleteShard(
			final String path,
			final long... shardPosition) throws N5Exception {

		final String shardPath = absoluteDataBlockPath(N5URI.normalizeGroupPath(path), shardPosition);
		if (getKeyValueAccess().isFile(shardPath)) {
			try {
				getKeyValueAccess().delete(shardPath);
				return true;
			} catch (final Exception e) {
				throw new N5Exception("The shard at " + 
						Arrays.toString(shardPosition) + 
						" could not be deleted.", e);
			}
		}
		return false;
	}

	@Override
	default boolean deleteBlock(
			final String path,
			final long... gridPosition) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		final DatasetAttributes datasetAttributes = getDatasetAttributes(normalPath);

		if (datasetAttributes == null) {
			return false; // Dataset doesn't exist - return true for consistency
		}

		// TODO
//		if (datasetAttributes.isSharded()) {
//			// For sharded datasets, we need to:
//			// 1. Find which shard contains this block
//			// 2. Read the shard
//			// 3. Remove the block from the shard
//			// 4. Write the shard back (or delete if empty)
//
//			final long[] shardPosition = datasetAttributes.getShardPositionForBlock(gridPosition);
//			final Shard<Object> shard = readShard(normalPath, datasetAttributes, shardPosition);
//
//			if (shard == null)
//				return false; // Shard doesn't exist, so block doesn't exist -
//								// return false for consistency
//
//			final int[] relativePosition = shard.getRelativeBlockPosition(gridPosition);
//			if (!shard.blockExists(relativePosition))
//				return false;
//
//			// Convert to InMemoryShard to manipulate blocks
//			final InMemoryShard<Object> inMemoryShard = InMemoryShard.fromShard(shard);
//
//			// Get all blocks except the one to remove
//			final List<DataBlock<Object>> remainingBlocks = new ArrayList<>();
//			for (DataBlock<Object> block : inMemoryShard.getBlocks()) {
//				if (!Arrays.equals(block.getGridPosition(), gridPosition)) {
//					remainingBlocks.add(block);
//				}
//			}
//
//			if (remainingBlocks.isEmpty()) {
//				// If no blocks remain, delete the entire shard
//				return deleteShard(normalPath, shardPosition);
//			} else {
//				// Create new shard with remaining blocks
//				final InMemoryShard<Object> newShard = new InMemoryShard<>(datasetAttributes, shardPosition);
//				for (DataBlock<Object> block : remainingBlocks) {
//					newShard.addBlock(block);
//				}
//
//				// Write the updated shard
//				writeShard(normalPath, datasetAttributes, newShard);
//				return true;
//			}
//
//		} else {
			// For non-sharded datasets, deleting the key deletes the block
			// and deleteShard deletes the key for gridPosition
			return deleteShard(path, gridPosition);
//		}
	}
}