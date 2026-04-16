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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;

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

	protected final KeyValueRoot keyValueRoot;
	protected final DelegateStore metaStore;
	protected final ContainerDialect store;
	protected final Gson gson;
	protected final boolean cacheMeta;

	/**
	 * Opens an {@link N5KeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueRoot
	 * 			  the backend key value access to use
	 * @param gsonBuilder
	 * 			  the GsonBuilder
	 * @param cacheMeta
	 *            cache attributes and metadata.
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other metadata that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and metadata by an
	 *            independent writer will not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5
	 *             version of the container is not compatible with this
	 *             implementation.
	 */
	public N5KeyValueReader(
			final KeyValueRoot keyValueRoot,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta)
			throws N5Exception {

		this(true, keyValueRoot, gsonBuilder, cacheMeta, true);
	}

	/**
	 * Opens an {@link N5KeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param checkVersion
	 *            if true, an N5IOException will be thrown if the container
	 *            (exists and) has an incompatible version
	 * @param keyValueRoot
	 *            the backend KeyValueAccess used
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
			final KeyValueRoot keyValueRoot,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta,
			final boolean checkExists)
			throws N5Exception {

		this.keyValueRoot = keyValueRoot;
		this.gson = registerGson(gsonBuilder).create();
		this.cacheMeta = cacheMeta;
		this.metaStore = createMetaStore(keyValueRoot, cacheMeta);
		this.store = createN5Store(metaStore, gson, cacheMeta);

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
		if (checkExists && (!versionFound && !exists("/")))
			throw new N5Exception.N5IOException("No container exists at " + keyValueRoot.uri());
	}

	protected GsonBuilder registerGson(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(DatasetAttributes.class, DatasetAttributes.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
		return gsonBuilder;
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
	public KeyValueRoot getKeyValueRoot() {

		return keyValueRoot;
	}

	@Override
	public ContainerDialect getN5Store() {

		return store;
	}

	@Override
	public boolean cacheMeta() {

		return cacheMeta;
	}
}
