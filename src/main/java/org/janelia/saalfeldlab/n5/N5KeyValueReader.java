package org.janelia.saalfeldlab.n5;

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

	protected final KeyValueRoot keyValueRoot;
	protected final HierarchyStore hierarchyStore;
	protected final ContainerDialect containerDialect;
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
		this.hierarchyStore = createHierarchyStore(keyValueRoot, cacheMeta);
		this.containerDialect = new N5Dialect(hierarchyStore, gson);

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
	public ContainerDialect getContainerDialect() {

		return containerDialect;
	}

	@Override
	public boolean cacheMeta() {

		return cacheMeta;
	}
}
