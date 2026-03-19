/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5;

import java.lang.reflect.Type;

import com.google.gson.JsonSyntaxException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * {@link N5Reader} implementation through {@link RootedKeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface CachedGsonKeyValueN5Reader extends GsonKeyValueN5Reader, N5JsonCacheableContainer {

	default N5JsonCache newCache() {

		return new N5JsonCache(this);
	}

	boolean cacheMeta();

	N5JsonCache getCache();

	@Override
	default JsonElement getAttributesFromContainer(final String normalPathName, final String normalCacheKey) {

		// This implementation doesn't use normalCacheKey, but rather depends on
		// getAttributesKey() being implemented.
		return GsonKeyValueN5Reader.super.getAttributes(normalPathName);
	}

	@Override
	default DatasetAttributes getDatasetAttributes(final String pathName) {

		if (!datasetExists(pathName))
			return null;

		final JsonElement attributes;
		if (cacheMeta()) {
			final String normalPath = N5GroupPath.of(pathName).normalPath();
			attributes = getCache().getAttributes(normalPath, getAttributesKey());
		} else {
			attributes = GsonKeyValueN5Reader.super.getAttributes(pathName);
		}

		return createDatasetAttributes(attributes);
	}

	@Override
	default <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws N5Exception {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);

		final JsonElement attributes;
		if (cacheMeta()) {
			final String normalPathName = N5GroupPath.of(pathName).normalPath();
			attributes = getCache().getAttributes(normalPathName, getAttributesKey());
		} else {
			attributes = GsonKeyValueN5Reader.super.getAttributes(pathName);
		}

		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
	}

	@Override
	default boolean exists(final String pathName) {

		if (cacheMeta()) {
			final String normalPathName = N5GroupPath.of(pathName).normalPath();
			return getCache().isGroup(normalPathName, getAttributesKey());
		} else {
			return existsFromContainer(pathName, null);
		}
	}

	@Override
	default boolean existsFromContainer(final String normalPathName, final String normalCacheKey) {

		final RootedKeyValueAccess kva = getRootedKeyValueAccess();
		final N5GroupPath normalPath = N5GroupPath.of(normalPathName);
		if (normalCacheKey == null)
			return kva.isDirectory(normalPath);
		else
			return kva.isFile(normalPath.resolve(normalCacheKey));
	}

	@Override
	default boolean groupExists(final String pathName) {

		final String normalPathName = N5GroupPath.of(pathName).normalPath();
		if (cacheMeta())
			return getCache().isGroup(normalPathName, null);
		else {
			return isGroupFromContainer(normalPathName);
		}
	}

	@Override
	default boolean isGroupFromContainer(final String normalPathName) {

		return GsonKeyValueN5Reader.super.groupExists(normalPathName);
	}

	@Override
	default boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {

		return true;
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5IOException {

		final String normalPathName = N5GroupPath.of(pathName).normalPath();
		if (cacheMeta()) {
			return getCache().isDataset(normalPathName, getAttributesKey());
		}
		return isDatasetFromContainer(normalPathName);
	}

	@Override
	default boolean isDatasetFromContainer(final String normalPathName) throws N5IOException {

		final JsonElement attributes = GsonKeyValueN5Reader.super.getAttributes(normalPathName);
		return createDatasetAttributes(attributes) != null;
	}

	@Override
	default boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {

		return isGroupFromAttributes(normalCacheKey, attributes) && createDatasetAttributes(attributes) != null;
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attribute
	 * @throws N5IOException if an IO error occurs while reading the attribute
	 */
	@Override
	default JsonElement getAttributes(final String pathName) throws N5IOException {

		final String normalPathName = N5GroupPath.of(pathName).normalPath();
		if (cacheMeta()) {
			return getCache().getAttributes(normalPathName, getAttributesKey());
		} else {
			return GsonKeyValueN5Reader.super.getAttributes(normalPathName);
		}
	}

	@Override
	default String[] list(final String pathName) throws N5IOException {

		final String normalPathName = N5GroupPath.of(pathName).normalPath();
		if (cacheMeta()) {
			return getCache().list(normalPathName);
		} else {
			return GsonKeyValueN5Reader.super.list(normalPathName);
		}
	}

	@Override
	default String[] listFromContainer(final String normalPathName) {

		return GsonKeyValueN5Reader.super.list(normalPathName);
	}
}
