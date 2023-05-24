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

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Filesystem {@link N5Writer} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public interface CachedGsonKeyValueWriter extends CachedGsonKeyValueReader, N5Writer {

	default void setVersion(final String path) throws IOException {

		final Version version = getVersion();
		if (!VERSION.isCompatible(version))
			throw new N5Exception.N5IOException("Incompatible version " + version + " (this is " + VERSION + ").");

		if (!VERSION.equals(version))
			setAttribute("/", VERSION_KEY, VERSION.toString());;
	}

	@Override
	default void createGroup(final String path) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		getKeyValueAccess().createDirectories(groupPath(normalPath));
		if (cacheMeta()) {
			 // check all nodes that are parents of the added node, if they have a children set, add the new child to it
			String[] pathParts = getKeyValueAccess().components(normalPath);
			String parent = N5URL.normalizeGroupPath("/");
			if (pathParts.length == 0) {
				pathParts = new String[]{""};
			}
			for (String child : pathParts) {

				final String childPath = getKeyValueAccess().compose(parent, child);
				getCache().forceAddNewCacheInfo(childPath, N5KeyValueReader.ATTRIBUTES_JSON, null, true, false );

				// only add if the parent exists and has children cached already
				if( parent != null && !child.isEmpty())
					getCache().addChildIfPresent(parent, child);

				parent = childPath;
			}
		}
	}

	/**
	 * Creates a dataset.  This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param pathName dataset path
	 * @param datasetAttributes the dataset attributes
	 * @throws IOException the exception
	 */
	@Override
	default void createDataset(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		// N5Writer.super.createDataset(pathName, datasetAttributes);
		/* the three lines below duplicate the single line above but would have to call
		 * normalizeGroupPath again the below duplicates code, but avoids extra work */
		final String normalPath = N5URL.normalizeGroupPath(pathName);
		createGroup(normalPath);
		setDatasetAttributes(normalPath, datasetAttributes);
	}

	/**
	 * Helper method that reads an existing JsonElement representing the root
	 * attributes for {@code normalGroupPath}, inserts and overrides the
	 * provided attributes, and writes them back into the attributes store.
	 *
	 * @param normalGroupPath to write the attributes to
	 * @param attributes      to write
	 * @throws IOException if unable to read the attributes at {@code normalGroupPath}
	 *                     <p>
	 *                     TODO consider cache (or you read the attributes twice?)
	 */
	default void writeAttributes(
			final String normalGroupPath,
			final JsonElement attributes) throws IOException {

		JsonElement root = getAttributes(normalGroupPath);
		root = GsonUtils.insertAttribute(root, "/", attributes, getGson());
		writeAndCacheAttributes(normalGroupPath, root);
	}

	/**
	 * Helper method that reads the existing map of attributes, JSON encodes,
	 * inserts and overrides the provided attributes, and writes them back into
	 * the attributes store.
	 *
	 * @param normalGroupPath to write the attributes to
	 * @param attributes      to write
	 * @throws IOException if unable to read the attributes at {@code normalGroupPath}
	 */
	default void writeAttributes(
			final String normalGroupPath,
			final Map<String, ?> attributes) throws IOException {

		if (attributes != null && !attributes.isEmpty()) {
			JsonElement existingAttributes = getAttributes(normalGroupPath);
			existingAttributes = existingAttributes != null && existingAttributes.isJsonObject()
					? existingAttributes.getAsJsonObject()
					: new JsonObject();
			JsonElement newAttributes = GsonUtils.insertAttributes(existingAttributes, attributes, getGson());
			writeAndCacheAttributes(normalGroupPath, newAttributes);
		}
	}

	default void writeAndCacheAttributes(final String normalGroupPath, final JsonElement attributes) throws IOException {

		try (final LockedChannel lock = getKeyValueAccess().lockForWriting(attributesPath(normalGroupPath))) {
			GsonUtils.writeAttributes(lock.newWriter(), attributes, getGson());
		}
		if (cacheMeta()) {
			JsonElement nullRespectingAttributes = attributes;
			/* Gson only filters out nulls when you write the JsonElement. This means it doesn't filter them out when caching.
			 * To handle this, we explicitly writer the existing JsonElement to a new JsonElement.
			 * The output is identical to the input if:
			 * 	- serializeNulls is true
			 * 	- no null values are present
			 * 	- cacheing is turned off */
			if (!getGson().serializeNulls()) {
				nullRespectingAttributes = getGson().toJsonTree(attributes);
			}
			/* Update the cache, and write to the writer */
			getCache().updateCacheInfo(normalGroupPath, N5KeyValueReader.ATTRIBUTES_JSON, nullRespectingAttributes);
		}
	}

	@Override
	default void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		if (!exists(normalPath))
			throw new N5Exception.N5IOException("" + normalPath + " is not a group or dataset.");

		writeAttributes(normalPath, attributes);
	}

	@Override
	default boolean removeAttribute(final String pathName, final String key) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		final String absoluteNormalPath = getKeyValueAccess().compose(getBasePath(), normalPath);
		final String normalKey = N5URL.normalizeAttributePath(key);

		if (!getKeyValueAccess().exists(absoluteNormalPath))
			return false;

		if (key.equals("/")) {
			writeAttributes(normalPath, JsonNull.INSTANCE);
			return true;
		}

		final JsonElement attributes = getAttributes(normalPath);
		if (GsonUtils.removeAttribute(attributes, normalKey) != null) {
			writeAttributes(normalPath, attributes);
			return true;
		}
		return false;
	}

	@Override
	default <T> T removeAttribute(final String pathName, final String key, final Class<T> cls) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		final String normalKey = N5URL.normalizeAttributePath(key);

		final JsonElement attributes = getAttributes(normalPath);
		final T obj = GsonUtils.removeAttribute(attributes, normalKey, cls, getGson());
		if (obj != null) {
			writeAttributes(normalPath, attributes);
		}
		return obj;
	}

	@Override
	default boolean removeAttributes(final String pathName, final List<String> attributes) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		boolean removed = false;
		for (final String attribute : attributes) {
			final String normalKey = N5URL.normalizeAttributePath(attribute);
			removed |= removeAttribute(normalPath, attribute);
		}
		return removed;
	}

	@Override
	default <T> void writeBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final String blockPath = getDataBlockPath(N5URL.normalizeGroupPath(path), dataBlock.getGridPosition());
		try (final LockedChannel lock = getKeyValueAccess().lockForWriting(blockPath)) {

			DefaultBlockWriter.writeBlock(lock.newOutputStream(), datasetAttributes, dataBlock);
		}
	}

	@Override
	default boolean remove(final String path) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		final String groupPath = groupPath(normalPath);
		if (getKeyValueAccess().exists(groupPath))
			getKeyValueAccess().delete(groupPath);

		if (cacheMeta()) {
			final String[] pathParts = getKeyValueAccess().components(normalPath);
			final String parent;
			if (pathParts.length <= 1) {
				parent = N5URL.normalizeGroupPath("/");
			} else {
				final int parentPathLength = pathParts.length - 1;
				parent = getKeyValueAccess().compose(Arrays.copyOf(pathParts, parentPathLength));
			}
			getCache().removeCache(parent, normalPath);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}

	@Override
	default boolean deleteBlock(
			final String path,
			final long... gridPosition) throws IOException {

		final String blockPath = getDataBlockPath(N5URL.normalizeGroupPath(path), gridPosition);
		if (getKeyValueAccess().exists(blockPath))
			getKeyValueAccess().delete(blockPath);

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}
}
