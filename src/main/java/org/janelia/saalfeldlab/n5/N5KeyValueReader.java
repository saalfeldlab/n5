/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
 * All rights reserved.
 *
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public class N5KeyValueReader implements GsonN5Reader {

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
		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
		this.gson = gsonBuilder.create();
		this.cacheMeta = cacheMeta;
		if (exists("/")) {
			final Version version = getVersion();
			if (!VERSION.isCompatible(version))
				throw new IOException("Incompatible version " + version + " (this is " + VERSION + ").");
		}
	}

	@Override public Gson getGson() {

		return gson;
	}

	/**
	 *
	 * @return N5 base path
	 */
	public String getBasePath() {

		return this.basePath;
	}


	/**
	 * Parses an attribute from the given attributes map.
	 *
	 * @param map
	 * @param key
	 * @param clazz
	 * @return
	 * @throws IOException
	 */
	protected <T> T parseAttribute(
			final HashMap<String, JsonElement> map,
			final String key,
			final Class<T> clazz) throws IOException {

		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, clazz);
		else
			return null;
	}

	/**
	 * Parses an attribute from the given attributes map.
	 *
	 * @param map
	 * @param key
	 * @param type
	 * @return
	 * @throws IOException
	 */
	protected <T> T parseAttribute(
			final HashMap<String, JsonElement> map,
			final String key,
			final Type type) throws IOException {

		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, type);
		else
			return null;
	}

	/**
	 * Reads the attributes map from a given {@link Reader}.
	 *
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	protected JsonElement readAttributes(final Reader reader) throws IOException {

		return GsonN5Reader.readAttributes(reader, gson);
	}

	@Override
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final long[] dimensions;
		final DataType dataType;
		int[] blockSize;
		Compression compression;
		final String compressionVersion0Name;
		final String normalPathName = N5URL.normalizePath(pathName);
		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return null;

			final JsonElement cachedMap;
			if (info.isDataset == null) {

				synchronized (info) {

					cachedMap = getCachedAttributes(info, normalPathName);
					if (cachedMap ==  null) {
						info.isDataset = false;
						return null;
					}

					dimensions = GsonN5Reader.readAttribute(cachedMap, DatasetAttributes.dimensionsKey, long[].class, gson);
					if (dimensions == null) {
						info.isDataset = false;
						return null;
					}

					dataType = GsonN5Reader.readAttribute(cachedMap, DatasetAttributes.dataTypeKey, DataType.class, gson);
					if (dataType == null) {
						info.isDataset = false;
						return null;
					}

					info.isDataset = true;
				}
			} else if (!info.isDataset) {
				return null;
			} else {

				cachedMap = getCachedAttributes(info, normalPathName);
				dimensions = GsonN5Reader.readAttribute(cachedMap, DatasetAttributes.dimensionsKey, long[].class, gson);
				dataType = GsonN5Reader.readAttribute(cachedMap, DatasetAttributes.dataTypeKey, DataType.class, gson);
			}

			blockSize = GsonN5Reader.readAttribute(cachedMap, DatasetAttributes.blockSizeKey, int[].class, gson);

			compression = GsonN5Reader.readAttribute(cachedMap, DatasetAttributes.compressionKey, Compression.class, gson);

			/* version 0 */
			compressionVersion0Name = compression
					== null
					? GsonN5Reader.readAttribute(cachedMap, DatasetAttributes.compressionTypeKey, String.class, gson)
					: null;


		} else {
			final JsonElement attributes = getAttributes(normalPathName);

			dimensions = GsonN5Reader.readAttribute(attributes, DatasetAttributes.dimensionsKey, long[].class, gson);
			if (dimensions == null)
				return null;

			dataType = GsonN5Reader.readAttribute(attributes, DatasetAttributes.dataTypeKey, DataType.class, gson);
			if (dataType == null)
				return null;

			blockSize = GsonN5Reader.readAttribute(attributes, DatasetAttributes.blockSizeKey, int[].class, gson);

			compression = GsonN5Reader.readAttribute(attributes, DatasetAttributes.compressionKey, Compression.class, gson);

			/* version 0 */
			compressionVersion0Name = compression == null
					? GsonN5Reader.readAttribute(attributes, DatasetAttributes.compressionTypeKey, String.class, gson)
					: null;
			}

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

	protected JsonElement readAttributes(final String absoluteNormalPath) throws IOException {

		if (!keyValueAccess.exists(absoluteNormalPath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absoluteNormalPath)) {
			return readAttributes(lockedChannel.newReader());
		}
	}

	/**
	 * Get and cache attributes for a group identified by an info object and a
	 * pathName.
	 *
	 * This helper method does not intelligently handle the case that the group
	 * does not exist (as indicated by info == emptyGroupInfo) which should be
	 * done in calling code.
	 *
	 * @param info
	 * @param pathName normalized group path without leading slash
	 * @return cached attributes
	 * 		empty map if the group exists but not attributes are set
	 * 		null if the group does not exist
	 * @throws IOException
	 */
	protected JsonElement getCachedAttributes(
			final N5GroupInfo info,
			final String normalPath) throws IOException {

		JsonElement metadataCache = info.attributesCache;
		if (metadataCache == null) {
			synchronized (info) {
				metadataCache = info.attributesCache;
				if (metadataCache == null) {
					final String absoluteNormalPath = attributesPath(normalPath);
					metadataCache = readAttributes(absoluteNormalPath);
					info.attributesCache = metadataCache;
				}
			}
		}
		return metadataCache;
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException {

		final String normalPathName = N5URL.normalizePath(pathName);
		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return null;
			final JsonElement metadataCache = getCachedAttributes(info, normalPathName);
			if (metadataCache == null)
				return null;
			return GsonN5Reader.readAttribute(metadataCache, N5URL.normalizeAttributePath(key), clazz, gson);
		} else {
			return GsonN5Reader.readAttribute(getAttributes(normalPathName), N5URL.normalizeAttributePath(key), clazz, gson);
		}
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws IOException {

		final String normalPathName = N5URL.normalizePath(pathName);
		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return null;
			final JsonElement metadataCache = getCachedAttributes(info, normalPathName);
			if (metadataCache == null)
				return null;
			return GsonN5Reader.readAttribute(metadataCache, N5URL.normalizeAttributePath(key), type, gson);
		} else {
			return GsonN5Reader.readAttribute(getAttributes(normalPathName), N5URL.normalizeAttributePath(key), type, gson);
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

		final String normalPathName = N5URL.normalizePath(pathName);
		if (cacheMeta)
			return getCachedN5GroupInfo(normalPathName) != emptyGroupInfo;
		else
			return groupExists(groupPath(normalPathName));
	}

	@Override
	public boolean datasetExists(final String pathName) throws IOException {

		if (cacheMeta) {
			final String normalPathName = N5URL.normalizePath(pathName);
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return false;
			if (info.isDataset == null) {
				synchronized (info) {
					if (info.isDataset == null ) {

					}
					else
						return info.isDataset;
				}
			} else
				return info.isDataset;
		}
		return exists(pathName) && getDatasetAttributes(pathName) != null;
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 */
	@Override public JsonElement getAttributes(final String pathName) throws IOException {

		final String path = attributesPath(N5URL.normalizePath(pathName));
		if (exists(pathName) && !keyValueAccess.exists(path))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path)) {
			return readAttributes(lockedChannel.newReader());
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final String path = getDataBlockPath(N5URL.normalizePath(pathName), gridPosition);
		if (!keyValueAccess.exists(path))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path)) {
			return DefaultBlockReader.readBlock(lockedChannel.newInputStream(), datasetAttributes, gridPosition);
		}
	}

	/**
	 *
	 * @param normalPath normalized path name
	 * @return
	 * @throws IOException
	 */
	protected String[] normalList(final String normalPath) throws IOException {

		return keyValueAccess.listDirectories(groupPath(normalPath));
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(N5URL.normalizePath(pathName));
			if (info == emptyGroupInfo)
				throw new IOException("Group '" + pathName +"' does not exist.");
			else {
				Set<String> children = info.children;
				final String[] list;
				if (children == null) {
					synchronized (info) {
						children = info.children;
						if (children == null) {
							list = normalList(N5URL.normalizePath(pathName));
							info.children = new HashSet<>(Arrays.asList(list));
						} else
							list = children.toArray(new String[children.size()]);
					}
				} else
					list = children.toArray(new String[children.size()]);

				return list;
			}
		} else {
			return normalList(N5URL.normalizePath(pathName));
		}
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid position.
	 *
	 * The returned path is
	 * <pre>
	 * $basePath/datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param normalDatasetPathName normalized dataset path without leading slash
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
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	protected String groupPath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath);
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the attributes
	 * file of a group or dataset.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	protected String attributesPath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, jsonFile);
	}

	/**
	 * Removes the leading slash from a given path and returns the normalized
	 * path.  It ensures correctness on both Unix and Windows, otherwise
	 * {@code pathName} is treated as UNC path on Windows, and
	 * {@code Paths.get(pathName, ...)} fails with
	 * {@code InvalidPathException}.
	 *
	 * @param path
	 * @return
	 */
	protected String normalize(final String path) {

		return keyValueAccess.normalize(path.startsWith("/") || path.startsWith("\\") ? path.substring(1) : path);
	}

	@Override
	public String toString() {

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, basePath);
	}
}
