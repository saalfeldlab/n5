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
package org.janelia.saalfeldlab.n5.cache;

import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.GsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.google.gson.JsonElement;

/**
 * An N5 container whose structure and attributes can be cached.
 * <p>
 * Implementations of interface methods must explicitly query the backing
 * storage unless noted otherwise. Cached implementations (e.g {@link CachedGsonKeyValueN5Reader}) call
 * these methods to update their {@link N5JsonCache}. Corresponding
 * {@link N5Reader} methods should use the cache, if present.
 */
public interface N5JsonCacheableContainer {

	/**
	 * Returns a {@link JsonElement} containing attributes at a given path,
	 * for a given cache key.
	 *
	 * @param normalPathName
	 *            the normalized path name
	 * @param normalCacheKey
	 *            the cache key
	 * @return the attributes as a json element.
	 * @see GsonKeyValueN5Reader#getAttributes
	 */
	JsonElement getAttributesFromContainer(final String normalPathName, final String normalCacheKey);

	/**
	 * Query whether a resource exists in this container.
	 *
	 * @param normalPathName
	 *            the normalized path name
	 * @param normalCacheKey
	 *            the normalized resource name (may be null).
	 * @return true if the resouce exists
	 */
	boolean existsFromContainer(final String normalPathName, final String normalCacheKey);

	/**
	 * Query whether a path in this container is a group.
	 *
	 * @param normalPathName
	 *            the normalized path name
	 * @return true if the path is a group
	 */
	boolean isGroupFromContainer(final String normalPathName);

	/**
	 * Query whether a path in this container is a dataset.
	 *
	 * @param normalPathName
	 *            the normalized path name
	 * @return true if the path is a dataset
	 * @see N5Reader#datasetExists
	 */
	boolean isDatasetFromContainer(final String normalPathName);

	/**
	 * 
	 * Returns true if a path is a group, given that the the given attributes exist
	 * for the given cache key.
	 * <p>
	 * Should not call the backing storage.
	 *
	 * @param normalCacheKey
	 *            the cache key
	 * @param attributes
	 *            the attributes
	 * @return true if the path is a group
	 */
	boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes);

	/**
	 * Returns true if a path is a dataset, given that the the given attributes exist
	 * for the given cache key.
	 * <p>
	 * Should not call the backing storage.
	 *
	 * @param normalCacheKey
	 *            the cache key
	 * @param attributes
	 *            the attributes
	 * @return true if the path is a dataset
	 */
	boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes);

	/**
	 * List the children of a path for this container.
	 *
	 * @param normalPathName
	 *            the normalized path name
	 * @return list of children
	 * @see N5Reader#list
	 */
	String[] listFromContainer(final String normalPathName);

}
