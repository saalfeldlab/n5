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

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;

/**
 * Cached default implementation of {@link N5Writer} with JSON attributes parsed
 * with {@link Gson}.
 */
public interface CachedGsonKeyValueN5Writer extends CachedGsonKeyValueN5Reader, GsonKeyValueN5Writer {

	@Override
	default void setVersion(final String path) throws N5Exception {

		final Version version = getVersion();
		if (!VERSION.isCompatible(version))
			throw new N5IOException("Incompatible version " + version + " (this is " + VERSION + ").");

		if (!VERSION.equals(version))
			setAttribute("/", VERSION_KEY, VERSION.toString());;
	}

	@Override
	default void createGroup(final String path) throws N5Exception {

		if (groupExists(path))
			return;

		final N5GroupPath group = N5GroupPath.of(path);
		getRootedKeyValueAccess().createDirectories(group);

		if (cacheMeta()) {
			// check all nodes that are parents of the added node, if they have
			// a children set, add the new child to it
			String[] pathParts = group.components();
			String parent = "";
			for (final String child : pathParts) {

				final String childPath = parent.isEmpty() ? child : parent + "/" + child;
				getCache().initializeNonemptyCache(childPath, getAttributesKey());
				getCache().updateCacheInfo(childPath, getAttributesKey());

				// Only add if the parent exists and has children cached already.
				// Note that the only reason to have child.isEmpty() is if the group is "".
				if (!child.isEmpty())
					getCache().addChildIfPresent(parent, child);

				parent = childPath;
			}
		}
	}

	@Override
	default void writeAttributes(
			final String normalGroupPath,
			final JsonElement attributes) throws N5Exception {

		writeAndCacheAttributes(normalGroupPath, attributes);
	}

	default void writeAndCacheAttributes(
			final String normalGroupPath,
			final JsonElement attributes) throws N5Exception {

		GsonKeyValueN5Writer.super.writeAttributes(normalGroupPath, attributes);

		if (cacheMeta()) {
			JsonElement nullRespectingAttributes = attributes;
			/*
			 * Gson only filters out nulls when you write the JsonElement. This
			 * means it doesn't filter them out when caching.
			 * To handle this, we explicitly writer the existing JsonElement to
			 * a new JsonElement.
			 * The output is identical to the input if:
			 * - serializeNulls is true
			 * - no null values are present
			 * - caching is turned off
			 */
			if (!getGson().serializeNulls()) {
				nullRespectingAttributes = getGson().toJsonTree(attributes);
			}
			/* Update the cache, and write to the writer */
			final String normalPathKey = N5GroupPath.of(normalGroupPath).normalPath();
			getCache().updateCacheInfo(normalPathKey, getAttributesKey(), nullRespectingAttributes);

			final N5GroupPath group = N5GroupPath.of(normalGroupPath);
			getMyCache().setAttributes(group, getAttributesKey(), nullRespectingAttributes);
		}
	}

	@Override
	default boolean remove(final String path) throws N5Exception {

		final N5GroupPath group = N5GroupPath.of(path);

		// GsonKeyValueN5Writer.super.remove(path)
		/*
		 * the lines below duplicate the single line above but would have to call
		 * normalizeGroupPath again the below duplicates code, but avoids extra work
		 */
		if (getRootedKeyValueAccess().isDirectory(group))
			getRootedKeyValueAccess().delete(group);

		if (cacheMeta()) {
			final N5GroupPath parent = group.parent();
			final String parentPath = parent == null ? null : parent.normalPath();
			getCache().removeCache(parentPath, group.normalPath());

			getMyCache().remove(group);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}
}
