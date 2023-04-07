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
import java.util.List;
import java.util.Map;

/**
 * Filesystem {@link N5Writer} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public interface GsonKeyValueWriter extends GsonKeyValueReader, N5Writer {

	default void setVersion(final String path) throws IOException {

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

	default void initializeGroup(String normalPath) {

	}

	@Override
	default void createGroup(final String path) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		getKeyValueAccess().createDirectories(groupPath(normalPath));
		initializeGroup(normalPath);
	}

	@Override
	default void createDataset(
			final String path,
			final DatasetAttributes datasetAttributes) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		createGroup(path);
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
		try (final LockedChannel lock = getKeyValueAccess().lockForWriting(attributesPath(normalGroupPath))) {
			GsonUtils.writeAttributes(lock.newWriter(), root, getGson());
		}
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
			try (final LockedChannel lock = getKeyValueAccess().lockForWriting(attributesPath(normalGroupPath))) {
				GsonUtils.writeAttributes(lock.newWriter(), newAttributes, getGson());
			}
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
