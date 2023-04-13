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
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;
import java.util.Arrays;

/**
 * {@link N5Writer} implementation through a {@link KeyValueAccess} with version compatibility check.
 * JSON attributes are parsed and written with {@link Gson}.
 *
 * @author Stephan Saalfeld
 */
public class N5KeyValueWriter extends N5KeyValueReader implements N5Writer {

	/**
	 * Opens an {@link N5KeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes. Storage is managed by 
	 * the given {@link KeyValueAccess}.
	 * <p>
	 * If the base path does not exist, it will be created.
	 * <p>
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param keyValueAccess the key value access
	 * @param basePath
	 *            n5 base path
	 * @param gsonBuilder the gson builder
	 * @param cacheAttributes
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes, this is most interesting for high
	 *            latency file systems. Changes of attributes by an independent
	 *            writer will not be tracked.
	 *
	 * @throws IOException
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5KeyValueWriter(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheAttributes)
			throws IOException {

		super(keyValueAccess, initializeContainer(keyValueAccess, basePath), gsonBuilder, cacheAttributes);
		createGroup("/");
		setVersion("/");
	}

	protected void setVersion(final String path) throws IOException {

		if (!VERSION.equals(getVersion()))
			setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	protected static String initializeContainer(
			final KeyValueAccess keyValueAccess,
			final String basePath) throws IOException {

		final String normBasePath = keyValueAccess.normalize(basePath);
		keyValueAccess.createDirectories(normBasePath);
		return normBasePath;
	}

	/**
	 * Performs any necessary initialization to ensure the key given by the
	 * argument {@code normalPath} is a valid group after creation. Called by
	 * {@link #createGroup(String)}.
	 *
	 * @param normalPath
	 *            the group path.
	 */
	protected void initializeGroup(final String normalPath) {

		// Nothing to do here, but other implementations (e.g. zarr) use this.
	}

	@Override
	public void createGroup(final String path) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		keyValueAccess.createDirectories(groupPath(normalPath));
		if (cacheMeta) {
			final String[] pathParts = keyValueAccess.components(normalPath);
			if (pathParts.length > 1) {
				String parent = N5URL.normalizeGroupPath("/");
				for (String child : pathParts) {
					final String childPath = keyValueAccess.compose(parent, child);
					cache.addChild(parent, child);
					parent = childPath;
				}
			}
		}
		initializeGroup(normalPath);
	}

	@Override
	public void createDataset(
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
	 * @param normalGroupPath
	 *            to write the attributes to
	 * @param attributes
	 *            to write
	 * @throws IOException
	 *             if unable to read the attributes at {@code normalGroupPath}
	 *
	 * TODO consider cache (or you read the attributes twice?)
	 */
	protected void writeAttributes(
			final String normalGroupPath,
			final JsonElement attributes) throws IOException {

		JsonElement root = getAttributes(normalGroupPath);
		root = GsonUtils.insertAttribute(root, "/", attributes, gson);
		writeAndCacheAttributes(normalGroupPath, root);
	}

	/**
	 * Helper method that reads the existing map of attributes, JSON encodes,
	 * inserts and overrides the provided attributes, and writes them back into
	 * the attributes store.
	 *
	 * @param normalGroupPath
	 *            to write the attributes to
	 * @param attributes
	 *            to write
	 * @throws IOException
	 *             if unable to read the attributes at {@code normalGroupPath}
	 */
	protected void writeAttributes(
			final String normalGroupPath,
			final Map<String, ?> attributes) throws IOException {

		if (attributes != null && !attributes.isEmpty()) {
			JsonElement existingAttributes = getAttributes(normalGroupPath);
			existingAttributes = existingAttributes != null && existingAttributes.isJsonObject()
					? existingAttributes.getAsJsonObject()
					: new JsonObject();
			JsonElement newAttributes = GsonUtils.insertAttributes(existingAttributes, attributes, gson);
			writeAndCacheAttributes(normalGroupPath, newAttributes);
		}
	}

	private void writeAndCacheAttributes(final String normalGroupPath, final JsonElement attributes) throws IOException {

		try (final LockedChannel lock = keyValueAccess.lockForWriting(attributesPath(normalGroupPath))) {
			GsonUtils.writeAttributes(lock.newWriter(), attributes, gson);
		}
		if (cacheMeta) {
			JsonElement nullRespectingAttributes = attributes;
			/* Gson only filters out nulls when you write the JsonElement. This means it doesn't filter them out when caching.
			* To handle this, we explicitly writer the existing JsonElement to a new JsonElement.
			* The output is identical to the input if:
			* 	- serializeNulls is true
			* 	- no null values are present
			* 	- cacheing is turned off */
			if (!gson.serializeNulls()) {
				nullRespectingAttributes = gson.toJsonTree(attributes);
			}
			/* Update the cache, and write to the writer */
			cache.updateCacheInfo(normalGroupPath, N5JsonCache.jsonFile, nullRespectingAttributes);
		}
	}

	@Override
	public void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		if (!exists(normalPath))
			throw new N5Exception.N5IOException("" + normalPath + " is not a group or dataset.");

		writeAttributes(normalPath, attributes);
	}

	@Override
	public boolean removeAttribute(final String pathName, final String key) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		final String absoluteNormalPath = keyValueAccess.compose(basePath, normalPath);
		final String normalKey = N5URL.normalizeAttributePath(key);

		if (!keyValueAccess.exists(absoluteNormalPath))
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
	public <T> T removeAttribute(final String pathName, final String key, final Class<T> cls) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		final String normalKey = N5URL.normalizeAttributePath(key);

		final JsonElement attributes = getAttributes(normalPath);
		final T obj = GsonUtils.removeAttribute(attributes, normalKey, cls, gson);
		if (obj != null) {
			writeAttributes(normalPath, attributes);
		}
		return obj;
	}

	@Override
	public boolean removeAttributes(final String pathName, final List<String> attributes) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		boolean removed = false;
		for (final String attribute : attributes) {
			final String normalKey = N5URL.normalizeAttributePath(attribute);
			removed |= removeAttribute(normalPath, attribute);
		}
		return removed;
	}

	@Override
	public <T> void writeBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final String blockPath = getDataBlockPath(N5URL.normalizeGroupPath(path), dataBlock.getGridPosition());
		try (final LockedChannel lock = keyValueAccess.lockForWriting(blockPath)) {

			DefaultBlockWriter.writeBlock(lock.newOutputStream(), datasetAttributes, dataBlock);
		}
	}

	@Override
	public boolean remove(final String path) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		final String groupPath = groupPath(normalPath);
		if (keyValueAccess.exists(groupPath))
			keyValueAccess.delete(groupPath);
		if (cacheMeta) {
			final String[] pathParts = keyValueAccess.components(normalPath);
			final String parent;
			if (pathParts.length <= 1) {
				parent = N5URL.normalizeGroupPath("/");
			} else {
				final int parentPathLength = pathParts.length - 1;
				parent = keyValueAccess.compose(Arrays.copyOf(pathParts, parentPathLength));
			}
			cache.removeCache(parent, normalPath);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}

	@Override
	public boolean deleteBlock(
			final String path,
			final long... gridPosition) throws IOException {

		final String blockPath = getDataBlockPath(N5URL.normalizeGroupPath(path), gridPosition);
		if (keyValueAccess.exists(blockPath))
			keyValueAccess.delete(blockPath);

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}
}
