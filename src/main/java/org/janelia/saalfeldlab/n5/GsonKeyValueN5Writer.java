/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.shard.Shard;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.janelia.saalfeldlab.n5.shard.VirtualShard;

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
			final String basePath) throws IOException {

		final String normBasePath = keyValueAccess.normalize(basePath);
		keyValueAccess.createDirectories(normBasePath);
		return normBasePath;
	}

	@Override
	default void createGroup(final String path) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		try {
			getKeyValueAccess().createDirectories(absoluteGroupPath(normalPath));
		} catch (final IOException | UncheckedIOException e) {
			throw new N5Exception.N5IOException("Failed to create group " + path, e);
		}
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
		} catch (final IOException | UncheckedIOException e) {
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
	default <T> void writeBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws N5Exception {

		/* Delegate to shard for writing block? How to know what type of shard? */
		if (datasetAttributes instanceof ShardedDatasetAttributes) {
			ShardedDatasetAttributes shardDatasetAttrs = (ShardedDatasetAttributes)datasetAttributes;
			final long[] shardPos = shardDatasetAttrs.getShardPositionForBlock(dataBlock.getGridPosition());
			final String shardPath = absoluteShardPath(N5URI.normalizeGroupPath(path), shardPos);
			final VirtualShard<T> shard = new VirtualShard<>(shardDatasetAttrs, shardPos, getKeyValueAccess(), shardPath);
			shard.writeBlock(dataBlock);
			return;
		}

		final String blockPath = absoluteDataBlockPath(N5URI.normalizeGroupPath(path), dataBlock.getGridPosition());
		try (final LockedChannel lock = getKeyValueAccess().lockForWriting(blockPath)) {
			try ( final OutputStream out = lock.newOutputStream()) {
				DefaultBlockWriter.writeBlock(out, datasetAttributes, dataBlock);
			}
		} catch (final IOException | UncheckedIOException e) {
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

		if (!(datasetAttributes instanceof ShardedDatasetAttributes))
			throw new N5IOException("Can not write shard into non-sharded dataset " + path);

		// TODO implement me
	}

	@Override
	default boolean remove(final String path) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		final String groupPath = absoluteGroupPath(normalPath);
		try {
			if (getKeyValueAccess().isDirectory(groupPath))
				getKeyValueAccess().delete(groupPath);
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to remove " + path, e);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}

	@Override
	default boolean deleteBlock(
			final String path,
			final long... gridPosition) throws N5Exception {

		final String blockPath = absoluteDataBlockPath(N5URI.normalizeGroupPath(path), gridPosition);
		try {
			if (getKeyValueAccess().isFile(blockPath))
				getKeyValueAccess().delete(blockPath);
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException(
					"Failed to delete block " + Arrays.toString(gridPosition) + " from dataset " + path,
					e);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}
}
