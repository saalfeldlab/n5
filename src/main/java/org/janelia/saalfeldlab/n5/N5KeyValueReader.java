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

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;

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
		public HashMap<String, Object> attributesCache = null;
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

	public Gson getGson() {

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
	protected HashMap<String, JsonElement> readAttributes(final Reader reader) throws IOException {

		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		final HashMap<String, JsonElement> map = gson.fromJson(reader, mapType);
		return map == null ? new HashMap<>() : map;
	}

	/**
	 * Inserts new the JSON export of attributes into the given attributes map.
	 *
	 * @param map
	 * @param attributes
	 * @param gson
	 * @throws IOException
	 */
	protected void insertAttributes(
			final HashMap<String, JsonElement> map,
			final Map<String, ?> attributes) throws IOException {

		for (final Entry<String, ?> entry : attributes.entrySet())
			map.put(entry.getKey(), gson.toJsonTree(entry.getValue()));
	}

	/**
	 * Return a reasonable class for a {@link JsonPrimitive}.  Possible return
	 * types are
	 * <ul>
	 * <li>boolean</li>
	 * <li>double</li>
	 * <li>long (if the number is an integer)</li>
	 * <li>String</li>
	 * <li>Object</li>
	 * </ul>
	 *
	 * @param jsonPrimitive
	 * @return
	 */
	protected static Class<?> classForJsonPrimitive(final JsonPrimitive jsonPrimitive) {

		if (jsonPrimitive.isBoolean())
			return boolean.class;
		else if (jsonPrimitive.isNumber()) {
			final Number number = jsonPrimitive.getAsNumber();
			if (number.longValue() == number.doubleValue())
				return long.class;
			else
				return double.class;
		} else if (jsonPrimitive.isString())
			return String.class;
		else return Object.class;
	}

	/**
	 * Best effort implementation of {@link N5Reader#listAttributes(String)}
	 * with limited type resolution fromJSON.  Possible return types are
	 * <ul>
	 * <li>null</li>
	 * <li>boolean</li>
	 * <li>double</li>
	 * <li>long (if the number is an integer)</li>
	 * <li>String</li>
	 * <li>Object</li>
	 * <li>boolean[]</li>
	 * <li>double[]</li>
	 * <li>long[] (if all numbers in the array are integers)</li>
	 * <li>String[]</li>
	 * <li>Object[]</li>
	 * </ul>
	 */
	@Override
	public Map<String, Class<?>> listAttributes(final String pathName) throws IOException {

		final HashMap<String, JsonElement> jsonElementMap = getAttributes(pathName);
		final HashMap<String, Class<?>> attributes = new HashMap<>();
		jsonElementMap.forEach(
				(key, jsonElement) -> {
					final Class<?> clazz;
					if (jsonElement.isJsonNull())
						clazz = null;
					else if (jsonElement.isJsonPrimitive())
						clazz = classForJsonPrimitive((JsonPrimitive)jsonElement);
					else if (jsonElement.isJsonArray()) {
						final JsonArray jsonArray = (JsonArray)jsonElement;
						Class<?> arrayElementClass = Object.class;
						if (jsonArray.size() > 0) {
							final JsonElement firstElement = jsonArray.get(0);
							if (firstElement.isJsonPrimitive()) {
								arrayElementClass = classForJsonPrimitive(firstElement.getAsJsonPrimitive());
								for (int i = 1; i < jsonArray.size() && arrayElementClass != Object.class; ++i) {
									final JsonElement element = jsonArray.get(i);
									if (element.isJsonPrimitive()) {
										final Class<?> nextArrayElementClass = classForJsonPrimitive(element.getAsJsonPrimitive());
										if (nextArrayElementClass != arrayElementClass)
											if (nextArrayElementClass == double.class && arrayElementClass == long.class)
												arrayElementClass = double.class;
											else {
												arrayElementClass = Object.class;
												break;
											}
									} else {
										arrayElementClass = Object.class;
										break;
									}
								}
							}
							clazz = Array.newInstance(arrayElementClass, 0).getClass();
						} else
							clazz = Object[].class;
					}
					else
						clazz = Object.class;
					attributes.put(key, clazz);
				});
		return attributes;
	}

	@Override
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final long[] dimensions;
		final DataType dataType;
		int[] blockSize;
		Compression compression;
		final String compressionVersion0Name;
		final String normalPathName = normalize(pathName);
		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return null;

			final HashMap<String, Object> cachedMap;
			if (info.isDataset == null) {

				synchronized (info) {

					cachedMap = getCachedAttributes(info, normalPathName);
					if (cachedMap.isEmpty()) {
						info.isDataset = false;
						return null;
					}

					dimensions = getAttribute(cachedMap, DatasetAttributes.dimensionsKey, long[].class);
					if (dimensions == null) {
						info.isDataset = false;
						return null;
					}

					dataType = getAttribute(cachedMap, DatasetAttributes.dataTypeKey, DataType.class);
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
				dimensions = getAttribute(cachedMap, DatasetAttributes.dimensionsKey, long[].class);
				dataType = getAttribute(cachedMap, DatasetAttributes.dataTypeKey, DataType.class);
			}

			blockSize = getAttribute(cachedMap, DatasetAttributes.blockSizeKey, int[].class);

			compression = getAttribute(cachedMap, DatasetAttributes.compressionKey, Compression.class);

			/* version 0 */
			compressionVersion0Name = compression
					== null
					? getAttribute(cachedMap, DatasetAttributes.compressionTypeKey, String.class)
					: null;


		} else {
			final HashMap<String, JsonElement> map = getAttributes(normalPathName);

			dimensions = parseAttribute(map, DatasetAttributes.dimensionsKey, long[].class);
			if (dimensions == null)
				return null;

			dataType = parseAttribute(map, DatasetAttributes.dataTypeKey, DataType.class);
			if (dataType == null)
				return null;

			blockSize = parseAttribute(map, DatasetAttributes.blockSizeKey, int[].class);

			compression = parseAttribute(map, DatasetAttributes.compressionKey, Compression.class);

			/* version 0 */
			compressionVersion0Name = compression == null
					? parseAttribute(map, DatasetAttributes.compressionTypeKey, String.class)
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

	protected HashMap<String, JsonElement> readAttributes(final String absoluteNormalPath) throws IOException {

		if (!keyValueAccess.exists(absoluteNormalPath))
			return new HashMap<>();

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
	protected HashMap<String, Object> getCachedAttributes(
			final N5GroupInfo info,
			final String normalPath) throws IOException {

		HashMap<String, Object> cachedMap = info.attributesCache;
		if (cachedMap == null) {
			synchronized (info) {
				cachedMap = info.attributesCache;
				if (cachedMap == null) {
					final String absoluteNormalPath = attributesPath(normalPath);
					cachedMap = new HashMap<>();
					final HashMap<String, JsonElement> map = readAttributes(absoluteNormalPath);
					cachedMap.putAll(map);
					info.attributesCache = cachedMap;
				}
			}
		}
		return cachedMap;
	}

	@SuppressWarnings("unchecked")
	protected <T> T getAttribute(
			final HashMap<String, Object> cachedMap,
			final String key,
			final Class<T> clazz) {

		final Object cachedAttribute = cachedMap.get(key);
		if (cachedAttribute == null)
			return null;
		else if (cachedAttribute instanceof JsonElement) {
			final T attribute = gson.fromJson((JsonElement)cachedAttribute, clazz);
			synchronized (cachedMap) {
				cachedMap.put(key, attribute);
			}
			return attribute;
		} else {
			return (T)cachedAttribute;
		}
	}

	/**
	 * Helper method that returns or JSON decodes a cached attribute.
	 *
	 * @param <T>
	 * @param cachedMap
	 * @param key
	 * @param type
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected <T> T getAttribute(
			final HashMap<String, Object> cachedMap,
			final String key,
			final Type type) {

		final Object cachedAttribute = cachedMap.get(key);
		if (cachedAttribute == null)
			return null;
		else if (cachedAttribute instanceof JsonElement) {
			final T attribute = gson.fromJson((JsonElement)cachedAttribute, type);
			synchronized (cachedMap) {
				cachedMap.put(key, attribute);
			}
			return attribute;
		} else {
			return (T)cachedAttribute;
		}
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException {

		final String normalPathName = normalize(pathName);
		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return null;
			final HashMap<String, Object> cachedMap = getCachedAttributes(info, normalPathName);
			if (cachedMap.isEmpty())
				return null;
			return getAttribute(cachedMap, key, clazz);
		} else {
			final HashMap<String, JsonElement> map = getAttributes(normalPathName);
			return parseAttribute(map, key, clazz);
		}
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws IOException {

		final String normalPathName = normalize(pathName);
		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return null;
			final HashMap<String, Object> cachedMap = getCachedAttributes(info, normalPathName);
			if (cachedMap.isEmpty())
				return null;
			return getAttribute(cachedMap, key, type);
		} else {
			final HashMap<String, JsonElement> map = getAttributes(normalPathName);
			return parseAttribute(map, key, type);
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

		final String normalPathName = normalize(pathName);
		if (cacheMeta)
			return getCachedN5GroupInfo(normalPathName) != emptyGroupInfo;
		else
			return groupExists(groupPath(normalPathName));
	}

	@Override
	public boolean datasetExists(final String pathName) throws IOException {

		if (cacheMeta) {
			final String normalPathName = normalize(pathName);
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
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {

		final String path = attributesPath(normalize(pathName));
		if (exists(pathName) && !keyValueAccess.exists(path))
			return new HashMap<>();

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path)) {
			return readAttributes(lockedChannel.newReader());
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final String path = getDataBlockPath(normalize(pathName), gridPosition);
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
			final N5GroupInfo info = getCachedN5GroupInfo(normalize(pathName));
			if (info == emptyGroupInfo)
				throw new IOException("Group '" + pathName +"' does not exist.");
			else {
				Set<String> children = info.children;
				final String[] list;
				if (children == null) {
					synchronized (info) {
						children = info.children;
						if (children == null) {
							list = normalList(normalize(pathName));
							info.children = new HashSet<>(Arrays.asList(list));
						} else
							list = children.toArray(new String[children.size()]);
					}
				} else
					list = children.toArray(new String[children.size()]);

				return list;
			}
		} else {
			return normalList(normalize(pathName));
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

		final StringBuilder builder = new StringBuilder(groupPath(normalPath));

		for (final long i : gridPosition) {
			builder.append("/");
			builder.append(i);
		}

		return builder.toString();
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the group or
	 * dataset.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	protected String groupPath(final String normalPath) {

		return basePath + "/" + normalPath;
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the attributes
	 * file of a group or dataset.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	protected String attributesPath(final String normalPath) {

		return groupPath(normalPath) + "/" + jsonFile;
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
