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

import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;

import com.google.gson.JsonSyntaxException;
import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;
import org.janelia.saalfeldlab.n5.cache.MyJsonCache;
import org.janelia.saalfeldlab.n5.cache.MyJsonCacheableContainer;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

/**
 * {@link N5Reader} implementation through {@link RootedKeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface CachedGsonKeyValueN5Reader extends GsonKeyValueN5Reader, N5JsonCacheableContainer, MyJsonCacheableContainer {

	boolean cacheMeta();

	N5JsonCache getCache();

	MyJsonCache getMyCache();

	default DelegateStore getDelegateStore() {
		return cacheMeta() ? getMyCache() : this;
	}

	@Override
	default JsonElement getAttributesFromContainer(final String normalPathName, final String normalCacheKey) {

		// This implementation doesn't use normalCacheKey, but rather depends on
		// getAttributesKey() being implemented.
		return GsonKeyValueN5Reader.super.getAttributes(normalPathName);
	}

	@Override
	default boolean exists(final String pathName) {

		boolean result = my_exists(pathName);
		// TODO: REPLACES OLD CACHE
		{
			// run old code as well to not mess with N5JsonCache
			if (cacheMeta()) {
				final String normalPathName = N5GroupPath.of(pathName).normalPath();
				getCache().isGroup(normalPathName, getAttributesKey());
			} else {
				existsFromContainer(pathName, null);
			}
		}
		return result;
	}
	// TODO: REVISED N5Reader
	// TODO: Can this throw exceptions?  If so, declare them!
	default boolean my_exists(final String pathName) {

		// NB: This method checks for existence of a group or dataset.
		//     For n5, every dataset must be a group, so checking for existence
		//     of a group is sufficient.
		final N5GroupPath group = N5GroupPath.of(pathName);
		if (cacheMeta()) {
			return getMyCache().isDirectory(group);
		} else {
			return my_isDirectoryFromContainer(group);
		}
	}

	// TODO: REVISED CacheableContainer
	@Override
	default boolean my_isDirectoryFromContainer(final N5GroupPath group) {
//		TODO: throws N5IOException?

		return getRootedKeyValueAccess().isDirectory(group);

//		TODO: final String attributesKey required? --> n5-zarr ???
//		if (attributesKey == null)
//			return kva.isDirectory(normalPath);
//		else
//			return kva.isFile(normalPath.resolve(normalCacheKey));
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

		// TODO: REPLACES OLD CACHE
		{
			// run old code as well to not mess with N5JsonCache
			final String normalPathName = N5GroupPath.of(pathName).normalPath();
			if (cacheMeta())
				getCache().isGroup(normalPathName, null);
			else {
				isGroupFromContainer(normalPathName);
			}
		}

		// NB: For n5, every directory is a group
		final N5GroupPath group = N5GroupPath.of(pathName);
		if (cacheMeta()) {
			return getMyCache().isDirectory(group);
		} else {
			return my_isDirectoryFromContainer(group);
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

		// TODO: REPLACES OLD CACHE
		{
			// run old code as well to not mess with N5JsonCache
			final String normalPathName = N5GroupPath.of(pathName).normalPath();
			if (cacheMeta()) {
				getCache().isDataset(normalPathName, getAttributesKey());
			} else {
				isDatasetFromContainer(normalPathName);
			}
		}

		return getDatasetAttributes(pathName) != null;
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

		return getDelegateStore().readAttributesJson(N5GroupPath.of(pathName), getAttributesKey());
	}





	@Override
	default String[] list(final String pathName) throws N5IOException {

		// TODO: REPLACES OLD CACHE
		final String[] result = my_list(pathName);
		{
			// run old code as well to not mess with N5JsonCache
			final String normalPathName = N5GroupPath.of(pathName).normalPath();
			if (cacheMeta()) {
				getCache().list(normalPathName);
			} else {
				GsonKeyValueN5Reader.super.list(normalPathName);
			}
		}
		return result;
	}
	// TODO: REVISED N5Reader
	default String[] my_list(final String pathName) throws N5IOException {
		final N5GroupPath group = N5GroupPath.of(pathName);
		if (cacheMeta()) {
			return getMyCache().list(group);
		} else {
			return my_listFromContainer(group);
		}
	}

	// TODO: REVISED CacheableContainer
	@Override
	default String[] my_listFromContainer(final N5GroupPath group) throws N5IOException {

		return getRootedKeyValueAccess().listDirectories(group);
	}

	@Override
	default String[] listFromContainer(final String normalPathName) {

		return GsonKeyValueN5Reader.super.list(normalPathName);
	}
}
