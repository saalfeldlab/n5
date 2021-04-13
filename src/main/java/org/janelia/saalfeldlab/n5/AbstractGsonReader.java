/**
 * Copyright (c) 2017, Stephan Saalfeld
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
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * Abstract base class implementing {@link N5Reader} with JSON attributes
 * parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public abstract class AbstractGsonReader implements GsonAttributesParser {

	protected final Gson gson;

	protected final HashMap<String, HashMap<String, Object>> attributesCache = new HashMap<>();

	protected final boolean cacheAttributes;

	/**
	 * Constructs an {@link AbstractGsonReader} with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param gsonBuilder
	 * @param cacheAttributes cache attributes
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes, this is most interesting for high latency
	 *    backends. Changes of attributes by an independent writer will not be
	 *    tracked.
	 */
	public AbstractGsonReader(final GsonBuilder gsonBuilder, final boolean cacheAttributes) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
		this.gson = gsonBuilder.create();
		this.cacheAttributes = cacheAttributes;
	}

	/**
	 * Constructs an {@link AbstractGsonReader} with a default
	 * {@link GsonBuilder}.
	 *
	 * @param cacheAttributes cache attributes
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes, this is most interesting for high latency
	 *    backends. Changes of attributes by an independent writer will not be
	 *    tracked.
		 */
	public AbstractGsonReader(final boolean cacheAttributes) {

		this(new GsonBuilder(), cacheAttributes);
	}

	/**
	 * Constructs an {@link AbstractGsonReader} with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param gsonBuilder
	 */
	public AbstractGsonReader(final GsonBuilder gsonBuilder) {

		this(gsonBuilder, false);
	}

	/**
	 * Constructs an {@link AbstractGsonReader} with a default
	 * {@link GsonBuilder}.
	 */
	public AbstractGsonReader() {

		this(new GsonBuilder(), false);
	}

	@Override
	public Gson getGson() {

		return gson;
	}

	@Override
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final long[] dimensions;
		final DataType dataType;
		int[] blockSize;
		Compression compression;
		final String compressionVersion0Name;
		if (cacheAttributes) {
			final HashMap<String, Object> cachedMap = getCachedAttributes(pathName);
			if (cachedMap.isEmpty())
				return null;

			dimensions = getAttribute(cachedMap, DatasetAttributes.dimensionsKey, long[].class);
			if (dimensions == null)
				return null;

			dataType = getAttribute(cachedMap, DatasetAttributes.dataTypeKey, DataType.class);
			if (dataType == null)
				return null;

			blockSize = getAttribute(cachedMap, DatasetAttributes.blockSizeKey, int[].class);

			compression = getAttribute(cachedMap, DatasetAttributes.compressionKey, Compression.class);

			/* version 0 */
			compressionVersion0Name = compression == null
					? getAttribute(cachedMap, DatasetAttributes.compressionTypeKey, String.class)
					: null;
		} else {
			final HashMap<String, JsonElement> map = getAttributes(pathName);

			dimensions = GsonAttributesParser.parseAttribute(map, DatasetAttributes.dimensionsKey, long[].class, gson);
			if (dimensions == null)
				return null;

			dataType = GsonAttributesParser.parseAttribute(map, DatasetAttributes.dataTypeKey, DataType.class, gson);
			if (dataType == null)
				return null;

			blockSize = GsonAttributesParser.parseAttribute(map, DatasetAttributes.blockSizeKey, int[].class, gson);

			compression = GsonAttributesParser.parseAttribute(map, DatasetAttributes.compressionKey, Compression.class, gson);

			/* version 0 */
			compressionVersion0Name = compression == null
					? GsonAttributesParser.parseAttribute(map, DatasetAttributes.compressionTypeKey, String.class, gson)
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

	/**
	 * Get and cache attributes for a group.
	 *
	 * @param pathName
	 * @return
	 * @throws IOException
	 */
	protected HashMap<String, Object> getCachedAttributes(final String pathName) throws IOException {

		HashMap<String, Object> cachedMap = attributesCache.get(pathName);
		if (cachedMap == null) {
			final HashMap<String, ?> map = getAttributes(pathName);
			cachedMap = new HashMap<>();
			if (map != null)
				cachedMap.putAll(map);

			synchronized (attributesCache) {
				attributesCache.put(pathName, cachedMap);
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

		if (cacheAttributes) {
			final HashMap<String, Object> cachedMap = getCachedAttributes(pathName);
			if (cachedMap.isEmpty())
				return null;
			return getAttribute(cachedMap, key, clazz);
		} else {
			final HashMap<String, JsonElement> map = getAttributes(pathName);
			return GsonAttributesParser.parseAttribute(map, key, clazz, getGson());
		}
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws IOException {

		if (cacheAttributes) {
			final HashMap<String, Object> cachedMap = getCachedAttributes(pathName);
			if (cachedMap.isEmpty())
				return null;
			return getAttribute(cachedMap, key, type);
		} else {
			final HashMap<String, JsonElement> map = getAttributes(pathName);
			return GsonAttributesParser.parseAttribute(map, key, type, getGson());
		}
	}
}
