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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public class N5KeyValueReaderWithInterfaces implements CachedGsonKeyValueReader {

	protected final KeyValueAccess keyValueAccess;

	protected final Gson gson;
	protected final boolean cacheMeta;

	protected final String basePath;
	private final N5JsonCache cache;

	/**
	 * Opens an {@link N5KeyValueReaderWithInterfaces} at a given base path with a custom
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
	public N5KeyValueReaderWithInterfaces(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) throws IOException {

		this.keyValueAccess = keyValueAccess;
		this.basePath = keyValueAccess.normalize(basePath);
		this.gson = GsonUtils.registerGson(gsonBuilder);
		this.cacheMeta = cacheMeta;
		this.cache = getCache();

		/* Existence checks, if any, go in subclasses */
		/* Check that version (if there is one) is compatible. */
		final Version version = getVersion();
		if (!VERSION.isCompatible(version))
			throw new N5Exception.N5IOException("Incompatible version " + version + " (this is " + VERSION + ").");
	}

	public Gson getGson() {

		return gson;
	}
	public KeyValueAccess getKeyValueAccess() {
		return keyValueAccess;
	}

	@Override
	public String getBasePath() {

		return this.basePath;
	}

	@Override public boolean cacheMeta() {

		return cacheMeta;
	}

	@Override public N5JsonCache getCache() {

		return this.cache;
	}
}
