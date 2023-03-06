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

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Filesystem {@link N5Writer} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public class N5KeyValueWriter extends N5KeyValueReader implements GsonN5Writer {

	/**
	 * Opens an {@link N5KeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param keyValueAccess
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 * @param cacheAttributes cache attributes
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes, this is most interesting for high latency file
	 *    systems. Changes of attributes by an independent writer will not be
	 *    tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5KeyValueWriter(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheAttributes) throws IOException {

		super(keyValueAccess, basePath, gsonBuilder, cacheAttributes);
		createGroup("/");
		if (!VERSION.equals(getVersion()))
			setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	/**
	 * Helper method to create and cache a group.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 * @throws IOException
	 */
	protected N5GroupInfo createCachedGroup(final String normalPath) throws IOException {

		N5GroupInfo info = getCachedN5GroupInfo(normalPath);
		if (info == emptyGroupInfo) {

			/* The directories may be created multiple times concurrently,
			 * but a new cache entry is inserted only if none has been
			 * inserted in the meantime (because that may already include
			 * more cached data).
			 *
			 * This avoids synchronizing on the cache for independent
			 * group creation.
			 */
			keyValueAccess.createDirectories(groupPath(normalPath));
			synchronized (metaCache) {
				info = getCachedN5GroupInfo(normalPath);
				if (info == emptyGroupInfo) {
					info = new N5GroupInfo();
					metaCache.put(normalPath, info);
				}
				for (String childPathName = normalPath; !(childPathName == null || childPathName.equals(""));) {
					final String parentPathName = keyValueAccess.parent(childPathName);
					if (parentPathName == null)
						break;
					N5GroupInfo parentInfo = getCachedN5GroupInfo(parentPathName);
					if (parentInfo == emptyGroupInfo) {
						parentInfo = new N5GroupInfo();
						parentInfo.isDataset = false;
						metaCache.put(parentPathName, parentInfo);
					}
					HashSet<String> children = parentInfo.children;
					if (children == null) {
						children = new HashSet<>();
					}
					synchronized (children) {
						children.add(
								keyValueAccess.relativize(childPathName, parentPathName));
						parentInfo.children = children;
					}
					childPathName = parentPathName;
				}
			}
		}
		return info;
	}

	@Override
	public void createGroup(final String path) throws IOException {

		final String normalPath = N5URL.normalizePath(path);
		if (cacheMeta) {
			final N5GroupInfo info = createCachedGroup(normalPath);
			synchronized (info) {
				if (info.isDataset == null)
					info.isDataset = false;
			}
		} else
			keyValueAccess.createDirectories(groupPath(normalPath));
	}

	@Override
	public void createDataset(
			final String path,
			final DatasetAttributes datasetAttributes) throws IOException {

		final String normalPath = N5URL.normalizePath(path);
		if (cacheMeta) {
			final N5GroupInfo info = createCachedGroup(normalPath);
			synchronized (info) {
				setDatasetAttributes(normalPath, datasetAttributes);
				info.isDataset = true;
			}
		} else {
			createGroup(path);
			setDatasetAttributes(normalPath, datasetAttributes);
		}
	}

	/**
	 * Helper method that reads the existing map of attributes, JSON encodes,
	 * inserts and overrides the provided attributes, and writes them back into
	 * the attributes store.
	 *
	 * @param path
	 * @param attributes
	 * @throws IOException
	 */
	protected void writeAttributes(
			final String path,
			final JsonElement attributes) throws IOException {

		try (final LockedChannel lock = keyValueAccess.lockForWriting(attributesPath(path))) {
			final JsonElement root = GsonN5Writer.insertAttribute(readAttributes(lock.newReader()), "/", attributes, gson);
			GsonN5Writer.writeAttributes(lock.newWriter(), root, gson);
		}
	}

	protected void writeAttributes(
			final String normalPath,
			final Map<String, ?> attributes) throws IOException {
		if (!attributes.isEmpty()) {
			createGroup(normalPath);
			final JsonElement existingAttributes = getAttributes(normalPath);
			JsonElement newAttributesJson = existingAttributes != null && existingAttributes.isJsonObject() ? existingAttributes.getAsJsonObject() : new JsonObject();
			newAttributesJson = GsonN5Writer.insertAttributes(newAttributesJson, attributes, gson);
			writeAttributes(normalPath, newAttributesJson);
		}
	}

	/**
	 * Check for attributes that are required for a group to be a dataset.
	 *
	 * @param cachedAttributes
	 * @return
	 */
	protected static boolean hasCachedDatasetAttributes(final JsonElement cachedAttributes) {


		if (cachedAttributes == null || !cachedAttributes.isJsonObject()) {
			return false;
		}

		final JsonObject metadataCache = cachedAttributes.getAsJsonObject();
		return metadataCache.has(DatasetAttributes.dimensionsKey) && metadataCache.has(DatasetAttributes.dataTypeKey);
	}


	/**
	 * Helper method to cache and write attributes.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @param attributes
	 * @return
	 * @throws IOException
	 */
	protected N5GroupInfo setCachedAttributes(
			final String normalPath,
			final Map<String, ?> attributes) throws IOException {

		N5GroupInfo info = getCachedN5GroupInfo(normalPath);
		if (info == emptyGroupInfo) {
			createGroup(normalPath);
			synchronized (metaCache) {
				info = getCachedN5GroupInfo(normalPath);
				if (info == emptyGroupInfo)
					throw new IOException("N5 group '" + normalPath + "' does not exist. Cannot set attributes.");
			}
		}
		final JsonElement metadataCache = getCachedAttributes(info, normalPath);
		synchronized (info) {
			/* Necessary ensure `nulls` are treated consistently regardless of reading from the cache or not */
			info.attributesCache = gson.toJsonTree(GsonN5Writer.insertAttributes(metadataCache, attributes, gson));
			writeAttributes(normalPath, info.attributesCache);
			info.isDataset = hasCachedDatasetAttributes(info.attributesCache);
		}
		return info;
	}

	@Override
	public void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws IOException {

		final String normalPath = N5URL.normalizePath(path);
		if (cacheMeta)
			setCachedAttributes(normalPath, attributes);
		else
			writeAttributes(normalPath, attributes);
	}

	@Override public boolean removeAttribute(String pathName, String key) throws IOException {

		final String normalPath = N5URL.normalizePath(pathName);
		final String normalKey = N5URL.normalizeAttributePath(key);


		if (cacheMeta) {
			N5GroupInfo info;
			synchronized (metaCache) {
				info = getCachedN5GroupInfo(normalPath);
				if (info == emptyGroupInfo) {
					throw new IOException("N5 group '" + normalPath + "' does not exist. Cannot set attributes.");
				}
			}
			final JsonElement metadataCache = getCachedAttributes(info, normalPath);
			if (GsonN5Writer.removeAttribute(metadataCache, normalKey) != null) {
				writeAttributes(normalPath, metadataCache);
				return true;
			}
		} else {
			final JsonElement attributes = getAttributes(normalPath);
			if (GsonN5Writer.removeAttribute(attributes, normalKey) != null) {
				writeAttributes(normalPath, attributes);
				return true;
			}
		}
		return false;
	}

	@Override public <T> T removeAttribute(String pathName, String key, Class<T> cls) throws IOException {
		final String normalPath = N5URL.normalizePath(pathName);
		final String normalKey = N5URL.normalizeAttributePath(key);

		if (cacheMeta) {
			N5GroupInfo info;
			synchronized (metaCache) {
				info = getCachedN5GroupInfo(normalPath);
				if (info == emptyGroupInfo) {
					throw new IOException("N5 group '" + normalPath + "' does not exist. Cannot set attributes.");
				}
			}
			final JsonElement metadataCache = getCachedAttributes(info, normalPath);
			final T obj = GsonN5Writer.removeAttribute(metadataCache, normalKey, cls, gson);
			if (obj != null) {
				writeAttributes(normalPath, metadataCache);
				return obj;
			}
		} else {
			final JsonElement attributes = getAttributes(normalPath);
			final T obj = GsonN5Writer.removeAttribute(attributes, normalKey, cls, gson);
			if (obj != null) {
				writeAttributes(normalPath, attributes);
				return obj;
			}
		}
		return null;
	}

	@Override public boolean removeAttributes(String pathName, List<String> attributes) throws IOException {
		final String normalPath = N5URL.normalizePath(pathName);
		boolean removed = false;
		for (String attribute : attributes) {
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

		final String blockPath = getDataBlockPath(N5URL.normalizePath(path), dataBlock.getGridPosition());
		try (final LockedChannel lock = keyValueAccess.lockForWriting(blockPath)) {

			DefaultBlockWriter.writeBlock(lock.newOutputStream(), datasetAttributes, dataBlock);
		}
	}

	@Override
	public boolean remove(final String path) throws IOException {

		final String normalPath = N5URL.normalizePath(path);
		final String groupPath = groupPath(normalPath);
		if (cacheMeta) {
			synchronized (metaCache) {
				if (keyValueAccess.exists(groupPath)) {
					keyValueAccess.delete(groupPath);

					/* cache nonexistence for all prior children */
					for (final String key : metaCache.keySet()) {
						if (key.startsWith(normalPath))
							metaCache.put(key, emptyGroupInfo);
					}

					/* remove child from parent */
					final String parentPath = keyValueAccess.parent(normalPath);
					final N5GroupInfo parent = metaCache.get(parentPath);
					if (parent != null) {
						final HashSet<String> children = parent.children;
						if (children != null) {
							synchronized (children) {
								children.remove(keyValueAccess.relativize(normalPath, parentPath));
							}
						}
					}
				}
			}
		} else {
			if (keyValueAccess.exists(groupPath))
				keyValueAccess.delete(groupPath);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}

	@Override
	public boolean deleteBlock(
			final String path,
			final long... gridPosition) throws IOException {

		final String blockPath = getDataBlockPath(N5URL.normalizePath(path), gridPosition);
		if (keyValueAccess.exists(blockPath))
			keyValueAccess.delete(blockPath);

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}
}
