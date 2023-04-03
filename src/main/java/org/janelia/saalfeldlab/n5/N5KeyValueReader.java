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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public class N5KeyValueReader implements N5Reader {

	/**
	 * Data object for caching meta data.  Elements that are null are not yet
	 * cached.
	 */
	protected static class N5GroupInfo {

		public HashSet<String> children = null;
		public JsonElement attributesCache = null;
		public Boolean isDataset = null;
	}

	protected static final N5GroupInfo emptyGroupInfo = new N5GroupInfo();

	protected final KeyValueAccess keyValueAccess;

	protected final Gson gson;

	protected final HashMap<String, N5GroupInfo> metaCache = new HashMap<>();

	protected final boolean cacheMeta;

	protected static final String jsonFile = "attributes.json";

	protected final String basePath;

	/**
	 * Opens an {@link N5KeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueAccess
	 * @param basePath N5 base path
	 * @param gsonBuilder
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

		this.keyValueAccess = keyValueAccess;
		this.basePath = keyValueAccess.normalize(basePath);
		this.gson = GsonUtils.registerGson(gsonBuilder);
		this.cacheMeta = cacheMeta;

		/* Existence checks, if any, go in subclasses */
		/* Check that version (if there is one) is compatible. */
		final Version version = getVersion();
		if (!VERSION.isCompatible(version))
			throw new IOException("Incompatible version " + version + " (this is " + VERSION + ").");
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

		final String normalPath = keyValueAccess.normalize(pathName);
		N5GroupInfo info = null;
		if (cacheMeta) {
			info = getCachedN5GroupInfo(normalPath);
			if (info == emptyGroupInfo || (info != null && info.isDataset != null && !info.isDataset))
				return null;
		}

		final JsonElement attributes = getAttributes(normalPath);

		final long[] dimensions = GsonUtils.readAttribute(attributes, DatasetAttributes.dimensionsKey, long[].class, gson);
		if (dimensions == null) {
			setGroupInfoIsDataset(info, false);
			return null;
		}

		final DataType dataType = GsonUtils.readAttribute(attributes, DatasetAttributes.dataTypeKey, DataType.class, gson);
		if (dataType == null) {
			setGroupInfoIsDataset(info, false);
			return null;
		}

		setGroupInfoIsDataset(info, true);

		final int[] blockSize = GsonUtils.readAttribute(attributes, DatasetAttributes.blockSizeKey, int[].class, gson);

		final Compression compression = GsonUtils.readAttribute(attributes, DatasetAttributes.compressionKey, Compression.class, gson);

		/* version 0 */
		final String compressionVersion0Name = compression
				== null
				? GsonUtils.readAttribute(attributes, DatasetAttributes.compressionTypeKey, String.class, gson)
				: null;

		return createDatasetAttributes(dimensions, dataType, blockSize, compression, compressionVersion0Name);
	}

	private void setGroupInfoIsDataset(final N5GroupInfo info, final boolean normalPathName) {
		if (info == null)
			return;
		synchronized (info) {
			if (info != null)
				info.isDataset = false;
		}
	}

	/**
	 * Get cached attributes for the group identified by a normalPath
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return cached attributes
	 * empty map if the group exists but no attributes are set
	 * null if the group does not exist, or if attributes have not been cached
	 */
	protected JsonElement getCachedAttributes(final String normalPath) {

		return metaCache.get(normalPath).attributesCache;
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException {

		final String normalPathName = keyValueAccess.normalize(pathName);
		final String normalizedAttributePath = N5URL.normalizeAttributePath(key);

		if (cacheMeta) {
			return getCachedAttribute(normalPathName, normalizedAttributePath, clazz);
		} else {
			return GsonUtils.readAttribute(getAttributes(normalPathName), normalizedAttributePath, clazz, gson);
		}
	}

	private <T> T getCachedAttribute(final String normalPathName, final String normalKey, final Class<T> clazz) throws IOException {

		return getCachedAttribute(normalPathName, normalKey, (Type) clazz);
	}

	private <T> T getCachedAttribute(final String normalPathName, final String normalKey, final Type type) throws IOException {

		final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
		if (info == emptyGroupInfo)
			return null;
		return GsonUtils.readAttribute(getAttributes(normalPathName), N5URL.normalizeAttributePath(normalKey), type, gson);
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws IOException {

		final String normalPathName = keyValueAccess.normalize(pathName);
		final String normalizedAttributePath = N5URL.normalizeAttributePath(key);
		if (cacheMeta) {
			return getCachedAttribute(normalPathName, key, type);
		} else {
			return GsonUtils.readAttribute(getAttributes(normalPathName), normalizedAttributePath, type, gson);
		}
	}

	protected boolean groupExists(final String absoluteNormalPath) {

		return keyValueAccess.exists(absoluteNormalPath) && keyValueAccess.isDirectory(absoluteNormalPath);
	}

	/**
	 * Get an existing cached N5 group info object or create it.
	 *
	 * @param normalPathName normalized group path without leading slash
	 * @return
	 */
	protected N5GroupInfo getCachedN5GroupInfo(final String normalPathName) {

		N5GroupInfo info = metaCache.get(normalPathName);
		if (info == null) {

			/* I do not have a better solution yet to allow parallel
			 * exists checks for independent paths than to accept the
			 * same exists check to potentially run multiple times.
			 */
			final boolean exists = groupExists(groupPath(normalPathName));

			synchronized (metaCache) {
				info = metaCache.get(normalPathName);
				if (info == null) {
					info = exists ? new N5GroupInfo() : emptyGroupInfo;
					metaCache.put(normalPathName, info);
				}
			}
		}
		return info;
	}

	@Override
	public boolean exists(final String pathName) {

		final String normalPathName = keyValueAccess.normalize(pathName);
		if (cacheMeta)
			return getCachedN5GroupInfo(normalPathName) != emptyGroupInfo;
		else
			try {
				return groupExists(groupPath(normalPathName)) || datasetExists(normalPathName);
			} catch (final IOException e) {
				return false;
			}
	}

	@Override
	public boolean datasetExists(final String pathName) throws IOException {

		if (cacheMeta) {
			final String normalPathName = keyValueAccess.normalize(pathName);
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return false;
			synchronized (info) {
				if (info.isDataset != null)
					return info.isDataset;
			}
		}
		// for n5, every dataset must be a group
		return groupExists(groupPath(pathName)) && getDatasetAttributes(pathName) != null;
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 */
	public JsonElement getAttributes(final String pathName) throws IOException {

		final String groupPath = keyValueAccess.normalize(pathName);
		final String attributesPath = attributesPath(groupPath);

		/* If cached, return the cache*/
		final N5GroupInfo groupInfo = getCachedN5GroupInfo(groupPath);
		if (cacheMeta) {
			if (groupInfo != null && groupInfo.attributesCache != null)
				return groupInfo.attributesCache;
		}

		if (exists(pathName) && !keyValueAccess.exists(attributesPath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(attributesPath)) {
			final JsonElement attributes = GsonUtils.readAttributes(lockedChannel.newReader(), gson);
			/* If we are reading from the access, update the cache*/
			groupInfo.attributesCache = attributes;
			return attributes;
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final String path = getDataBlockPath(keyValueAccess.normalize(pathName), gridPosition);
		if (!keyValueAccess.exists(path))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path)) {
			return DefaultBlockReader.readBlock(lockedChannel.newInputStream(), datasetAttributes, gridPosition);
		}
	}

	/**
	 * @param normalPath normalized path name
	 * @return the normalized groups in {@code normalPath}
	 * @throws IOException
	 */
	protected String[] normalList(final String normalPath) throws IOException {

		return keyValueAccess.listDirectories(groupPath(normalPath));
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final String normalPath = keyValueAccess.normalize(pathName);
		if (cacheMeta) {
			return getCachedList(normalPath);
		} else {
			return normalList(normalPath);
		}
	}

	private String[] getCachedList(final String normalPath) throws IOException {

		final N5GroupInfo info = getCachedN5GroupInfo(normalPath);
		if (info == emptyGroupInfo)
			throw new IOException("Group '" + normalPath + "' does not exist.");
		else {

			if (info.children != null)
				return (info.children).toArray(new String[(info.children).size()]);

			synchronized (info) {
				final String[] list = normalList(normalPath);
				info.children = new HashSet<>(Arrays.asList(list));
				return list;
			}
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
	 * @param gridPosition
	 * @return
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

	/**
	 * Constructs the absolute path (in terms of this store) for the attributes
	 * file of a group or dataset.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return the absolute path to the attributes
	 */
	protected String attributesPath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, jsonFile);
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
	 * @return if {@link DatasetAttributes#dimensionsKey} and {@link DatasetAttributes#dataTypeKey} are present
	 */
	protected static boolean hasDatasetAttributes(final JsonElement attributes) {

		if (attributes == null || !attributes.isJsonObject()) {
			return false;
		}

		final JsonObject metadataCache = attributes.getAsJsonObject();
		return metadataCache.has(DatasetAttributes.dimensionsKey) && metadataCache.has(DatasetAttributes.dataTypeKey);
	}
}
