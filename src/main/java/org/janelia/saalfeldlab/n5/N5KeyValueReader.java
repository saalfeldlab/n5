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
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public class N5KeyValueReader implements N5Reader, N5JsonCacheableContainer {

	protected final KeyValueAccess keyValueAccess;

	protected final Gson gson;

	protected final N5JsonCache cache = new N5JsonCache(this);

	protected final boolean cacheMeta;

	protected final String basePath;

	/**
	 * Opens an {@link N5KeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes. Storage is managed by 
	 * the given {@link KeyValueAccess}.
	 *
	 * @param keyValueAccess the key value access
	 * @param basePath N5 base path
	 * @param gsonBuilder the gson builder
	 * @param cacheMeta cache attributes and meta data
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes and other meta data that requires accessing the
	 *    store. This is most interesting for high latency backends. Changes
	 *    of cached attributes and meta data by an independent writer will
	 *    not be tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be read or does not exist,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5KeyValueReader(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) throws IOException {

		this(true, keyValueAccess, basePath, gsonBuilder, cacheMeta);
	}

	protected N5KeyValueReader(
			final boolean checkVersion,
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) throws IOException {

		this.keyValueAccess = keyValueAccess;
		this.basePath = keyValueAccess.normalize(basePath);
		this.gson = GsonUtils.registerGson(gsonBuilder);
		this.cacheMeta = cacheMeta;

		if( checkVersion ) {
			/* Existence checks, if any, go in subclasses */
			/* Check that version (if there is one) is compatible. */
			final Version version = getVersion();
			if (!VERSION.isCompatible(version))
				throw new N5Exception.N5IOException("Incompatible version " + version + " (this is " + VERSION + ").");
		}
	}

	public Gson getGson() {

		return gson;
	}

	@Override
	public Map<String, Class<?>> listAttributes(final String pathName) throws IOException {

		return GsonUtils.listAttributes(getAttributes(pathName));
	}

	public KeyValueAccess getKeyValueAccess() {
		return keyValueAccess;
	}

	@Override
	public String getBasePath() {

		return this.basePath;
	}

	@Override
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		final JsonElement attributes;
		if (cacheMeta && cache.isDataset(normalPath, N5JsonCache.jsonFile)) {
			attributes = cache.getAttributes(normalPath, N5JsonCache.jsonFile);
		} else {
			attributes = getAttributes(normalPath);
		}

		return createDatasetAttributes(attributes);
	}

	private DatasetAttributes normalGetDatasetAttributes(final String pathName) throws N5Exception.N5IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		final JsonElement attributes = getAttributesFromContainer(normalPath);
		return createDatasetAttributes(attributes);
	}

	private DatasetAttributes createDatasetAttributes(JsonElement attributes) {

		final long[] dimensions = GsonUtils.readAttribute(attributes, DatasetAttributes.DIMENSIONS_KEY, long[].class, gson);
		if (dimensions == null) {
			return null;
		}

		final DataType dataType = GsonUtils.readAttribute(attributes, DatasetAttributes.DATA_TYPE_KEY, DataType.class, gson);
		if (dataType == null) {
			return null;
		}

		final int[] blockSize = GsonUtils.readAttribute(attributes, DatasetAttributes.BLOCK_SIZE_KEY, int[].class, gson);

		final Compression compression = GsonUtils.readAttribute(attributes, DatasetAttributes.COMPRESSION_KEY, Compression.class, gson);

		/* version 0 */
		final String compressionVersion0Name = compression
				== null
				? GsonUtils.readAttribute(attributes, DatasetAttributes.compressionTypeKey, String.class, gson)
				: null;

		return createDatasetAttributes(dimensions, dataType, blockSize, compression, compressionVersion0Name);
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException {

		final String normalPathName = N5URL.normalizeGroupPath(pathName);
		final String normalizedAttributePath = N5URL.normalizeAttributePath(key);

		final JsonElement attributes;
		if (cacheMeta) {
			attributes = cache.getAttributes(normalPathName, N5JsonCache.jsonFile);
		} else {
			attributes = getAttributes(normalPathName);
		}
		return GsonUtils.readAttribute(attributes, normalizedAttributePath, clazz, gson);
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws IOException {

		final String normalPathName = N5URL.normalizeGroupPath(pathName);
		final String normalizedAttributePath = N5URL.normalizeAttributePath(key);
		JsonElement attributes;
		if (cacheMeta) {
			attributes = cache.getAttributes(normalPathName, N5JsonCache.jsonFile);
		} else {
			attributes = getAttributes(normalPathName);
		}
		return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, gson);
	}

	public boolean isGroupFromContainer(final String absoluteNormalPath) {

		return keyValueAccess.exists(absoluteNormalPath) && keyValueAccess.isDirectory(absoluteNormalPath);
	}

	@Override
	public boolean exists(final String pathName) {

		final String normalPathName = N5URL.normalizeGroupPath(pathName);
		if (cacheMeta)
			return cache.exists(normalPathName, N5JsonCache.jsonFile);
		else {
			return existsFromContainer(normalPathName);
		}
	}

	public boolean existsFromContainer(String normalPathName) {

		return keyValueAccess.exists(groupPath(normalPathName));
	}

	@Override
	public boolean datasetExists(final String pathName) throws N5Exception.N5IOException {

		if (cacheMeta) {
			final String normalPathName = N5URL.normalizeGroupPath(pathName);
			return cache.isDataset(normalPathName, N5JsonCache.jsonFile);
		}
		return isDatasetFromContainer(pathName);
	}

	public boolean isDatasetFromContainer(String pathName) throws N5Exception.N5IOException {

		// for n5, every dataset must be a group
		return isGroupFromContainer(groupPath(pathName)) && normalGetDatasetAttributes(pathName) != null;
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return the json element
	 * @throws IOException the exception
	 */
	public JsonElement getAttributes(final String pathName) throws IOException {

		final String groupPath = N5URL.normalizeGroupPath(pathName);

		/* If cached, return the cache*/
		if (cacheMeta) {
			return cache.getAttributes(groupPath, N5JsonCache.jsonFile);
		} else {
			return getAttributesFromContainer(groupPath);
		}

	}

	public JsonElement getAttributesFromContainer(String groupPath ) throws N5Exception.N5IOException {

		return getAttributesFromContainer(groupPath, N5JsonCache.jsonFile);
	}

	public JsonElement getAttributesFromContainer(String groupPath, String cachePath) throws N5Exception.N5IOException {

		final String attributesPath = attributesPath(groupPath);
		if (!existsFromContainer(groupPath))
			throw new N5Exception.N5IOException("Group " + groupPath + " does not exist");
		if (!keyValueAccess.exists(attributesPath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(attributesPath)) {
			return GsonUtils.readAttributes(lockedChannel.newReader(), gson);
		} catch (IOException e) {
			throw new N5Exception.N5IOException("Cannot open lock for Reading", e);
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final String path = getDataBlockPath(N5URL.normalizeGroupPath(pathName), gridPosition);
		if (!keyValueAccess.exists(path))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path)) {
			return DefaultBlockReader.readBlock(lockedChannel.newInputStream(), datasetAttributes, gridPosition);
		}
	}

	/**
	 * @param normalPath normalized path name
	 * @return the normalized groups in {@code normalPath}
	 * @throws N5Exception.N5IOException the exception
	 */
	public String[] listFromContainer(final String normalPath) throws N5Exception.N5IOException {

		try {
			return keyValueAccess.listDirectories(groupPath(normalPath));
		} catch (IOException e) {
			throw new N5Exception.N5IOException("Cannot list directories for group " + normalPath, e);
		}
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		if (cacheMeta) {
			return cache.list(normalPath);
		} else {
			return listFromContainer(normalPath);
		}
	}


	/**
	 * Constructs the path for a data block in a dataset at a given grid position.
	 * <p>
	 * The returned path is
	 * <pre>
	 * $basePath/datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 * <p>
	 * This is the file into which the data block will be stored.
	 *
	 * @param normalPath normalized dataset path
	 * @param gridPosition the grid position
	 * @return the block path
	 */
	protected String getDataBlockPath(
			final String normalPath,
			final long... gridPosition) {

		final String[] components = new String[gridPosition.length + 2];
		components[0] = basePath;
		components[1] = normalPath;
		int i = 1;
		for (final long p : gridPosition)
			components[++i] = Long.toString(p);

		return keyValueAccess.compose(components);
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the group or
	 * dataset.
	 *
	 * @param normalGroupPath normalized group path without leading slash
	 * @return the absolute path to the group
	 */
	protected String groupPath(final String normalGroupPath) {

		return keyValueAccess.compose(basePath, normalGroupPath);
	}

	@Override
	public String groupPath(String... nodes) {

		// alternatively call compose twice, once with this functions inputs, then pass the result to the other groupPath method 
		// this impl assumes streams and array building are less expensive than keyValueAccess composition (may not always be true)
		return keyValueAccess.compose(Stream.concat(Stream.of(basePath), Arrays.stream(nodes)).toArray(String[]::new));
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the attributes
	 * file of a group or dataset.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return the absolute path to the attributes
	 */
	protected String attributesPath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, N5JsonCache.jsonFile);
	}

	@Override
	public String toString() {

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, basePath);
	}

	private static DatasetAttributes createDatasetAttributes(
			final long[] dimensions,
			final DataType dataType,
			int[] blockSize,
			Compression compression,
			final String compressionVersion0Name
	) {

		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		/* version 0 */
		if (compression == null) {
			switch (compressionVersion0Name) {
			case "raw":
				compression = new RawCompression();
				break;
			case "gzip":
				compression = new GzipCompression();
				break;
			case "bzip2":
				compression = new Bzip2Compression();
				break;
			case "lz4":
				compression = new Lz4Compression();
				break;
			case "xz":
				compression = new XzCompression();
				break;
			}
		}

		return new DatasetAttributes(dimensions, blockSize, dataType, compression);
	}

	/**
	 * Check for attributes that are required for a group to be a dataset.
	 *
	 * @param attributes to check for dataset attributes
	 * @return if {@link DatasetAttributes#DIMENSIONS_KEY} and {@link DatasetAttributes#DATA_TYPE_KEY} are present
	 */
	protected static boolean hasDatasetAttributes(final JsonElement attributes) {

		if (attributes == null || !attributes.isJsonObject()) {
			return false;
		}

		final JsonObject metadataCache = attributes.getAsJsonObject();
		return metadataCache.has(DatasetAttributes.DIMENSIONS_KEY) && metadataCache.has(DatasetAttributes.DATA_TYPE_KEY);
	}
}
