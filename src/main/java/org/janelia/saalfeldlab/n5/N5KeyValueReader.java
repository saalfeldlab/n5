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

import java.net.URI;
import java.net.URISyntaxException;

import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public class N5KeyValueReader implements CachedGsonKeyValueN5Reader {

	public static final String ATTRIBUTES_JSON = "attributes.json";

	protected final KeyValueAccess keyValueAccess;

	protected final Gson gson;
	protected final boolean cacheMeta;
	protected URI uri;

	private final N5JsonCache cache;

	/**
	 * Opens an {@link N5KeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueAccess
	 * 			  the KeyValueAccess backend used
	 * @param basePath
	 *            N5 base path
	 * @param gsonBuilder
	 * 			  the GsonBuilder
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoidsfrequent reading and parsing of
	 *            JSON encoded attributes andother meta data that requires
	 *            accessing the store. This ismost interesting for high latency
	 *            backends. Changes of cachedattributes and meta data by an
	 *            independent writer will not betracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5
	 *             version of the container is not compatible with this
	 *             implementation.
	 */
	public N5KeyValueReader(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta)
			throws N5Exception {

		this(true, keyValueAccess, basePath, gsonBuilder, cacheMeta, true);
	}

	/**
	 * Opens an {@link N5KeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param checkVersion
	 *            the version check
	 * @param keyValueAccess
	 *            the backend KeyValueAccess used
	 * @param basePath
	 *            base path
	 * @param gsonBuilder
	 *            the GsonBuilder
	 * @param cacheMeta
	 *            cache attributes and meta data Setting this to true avoids
	 *            frequent reading and parsing of JSON encoded attributes and
	 *            other meta data that requires accessing the store. This is
	 *            most interesting for high latency backends. Changes of cached
	 *            attributes and meta data by an independent writer will not be
	 *            tracked.
	 * @param checkExists
	 *            if true, an N5IOException will be thrown if a container does
	 *            not exist at the specified location
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5
	 *             version of the container is not compatible with this
	 *             implementation.
	 */
	protected N5KeyValueReader(
			final boolean checkVersion,
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta,
			final boolean checkExists)
			throws N5Exception {

		this.keyValueAccess = keyValueAccess;
		this.gson = GsonUtils.registerGson(gsonBuilder);
		this.cacheMeta = cacheMeta;

		if (this.cacheMeta)
			this.cache = newCache();
		else
			this.cache = null;

		try {
			uri = keyValueAccess.uri(basePath);
		} catch (final URISyntaxException e) {
			throw new N5Exception(e);
		}

		boolean versionFound = false;
		if (checkVersion) {
			/* Existence checks, if any, go in subclasses */
			/* Check that version (if there is one) is compatible. */
			final Version version = getVersion();
			versionFound = !version.equals(NO_VERSION);
			if (!VERSION.isCompatible(version))
				throw new N5Exception.N5IOException(
					"Incompatible version " + version + " (this is " + VERSION + ").");
		}

		// if a version was found, the container exists - don't need to check again
		if (checkExists && (!versionFound && !inferExistence("/")))
			throw new N5Exception.N5IOException("No container exists at " + basePath);
	}

	private boolean inferExistence(String path) {

		final JsonElement attributes = getAttributes(path);
		return attributes != null || exists(path);
	}

	@Override
	public String getAttributesKey() {

		return ATTRIBUTES_JSON;
	}

	@Override
	public Gson getGson() {

		return gson;
	}

	@Override
	public KeyValueAccess getKeyValueAccess() {

		return keyValueAccess;
	}

	@Override
	public URI getURI() {

		return uri;
	}

	@Override
	public boolean cacheMeta() {

		return cacheMeta;
	}

	@Override
	public N5JsonCache getCache() {

		return this.cache;
	}

}
