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
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

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

		final String normalPath = N5URL.normalizeGroupPath(path);
		// TODO: John document this!
		// if you are a group, avoid hitting the backend
		// if something exists, be safe
		if (cacheMeta()) {
			if (getCache().isGroup(normalPath, N5KeyValueReader.ATTRIBUTES_JSON))
				return;
			else if (getCache().exists(normalPath, N5KeyValueReader.ATTRIBUTES_JSON)) {
				throw new N5Exception("Can't make a group on existing path.");
			}
		}

		// N5Writer.super.createGroup(path);
		/*
		 * the 6 lines below duplicate the single line above but would have to
		 * call
		 * normalizeGroupPath again the below duplicates code, but avoids extra
		 * work
		 */
		try {
			getKeyValueAccess().createDirectories(groupPath(normalPath));
		} catch (final IOException e) {
			throw new N5Exception.N5IOException("Failed to create group " + path, e);
		}

		if (cacheMeta()) {
			// check all nodes that are parents of the added node, if they have
			// a children set, add the new child to it
			String[] pathParts = getKeyValueAccess().components(normalPath);
			String parent = N5URL.normalizeGroupPath("/");
			if (pathParts.length == 0) {
				pathParts = new String[]{""};
			}
			for (final String child : pathParts) {

				final String childPath = parent.isEmpty() ? child : parent + "/" + child;
				getCache().initializeNonemptyCache(childPath, N5KeyValueReader.ATTRIBUTES_JSON);
				getCache().updateCacheInfo(childPath, N5KeyValueReader.ATTRIBUTES_JSON);

				// only add if the parent exists and has children cached already
				if (parent != null && !child.isEmpty())
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
			 * - cacheing is turned off
			 */
			if (!getGson().serializeNulls()) {
				nullRespectingAttributes = getGson().toJsonTree(attributes);
			}
			/* Update the cache, and write to the writer */
			getCache().updateCacheInfo(normalGroupPath, N5KeyValueReader.ATTRIBUTES_JSON, nullRespectingAttributes);
		}
	}

	@Override
	default boolean remove(final String path) throws N5Exception {

		// GsonKeyValueN5Writer.super.remove(path)
		/*
		 * the 8 lines below duplicate the single line above but would have to
		 * call
		 * normalizeGroupPath again the below duplicates code, but avoids extra
		 * work
		 */
		final String normalPath = N5URL.normalizeGroupPath(path);
		final String groupPath = groupPath(normalPath);
		try {
			if (getKeyValueAccess().isDirectory(groupPath))
				getKeyValueAccess().delete(groupPath);
		} catch (final IOException e) {
			throw new N5IOException("Failed to remove " + path, e);
		}

		if (cacheMeta()) {
			final String[] pathParts = getKeyValueAccess().components(normalPath);
			final String parent;
			if (pathParts.length <= 1) {
				parent = N5URL.normalizeGroupPath("/");
			} else {
				final int parentPathLength = pathParts.length - 1;
				parent = getKeyValueAccess().compose(Arrays.copyOf(pathParts, parentPathLength));
			}
			getCache().removeCache(parent, normalPath);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}
}
