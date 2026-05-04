package org.janelia.saalfeldlab.n5;

import java.nio.file.FileSystems;

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
//public class N5FSReader extends N5KeyValueReader {
public class N5FSReader extends N5KeyValueReader {

	/**
	 * Opens an {@link N5FSReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param basePath
	 *            N5 base path
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer on the same container will not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5
	 *             version of the container is not compatible with this
	 *             implementation.
	 */
	public N5FSReader(final String basePath, final GsonBuilder gsonBuilder, final boolean cacheMeta)
			throws N5Exception {

		super(
				new FileSystemKeyValueAccess(),
				basePath,
				gsonBuilder,
				cacheMeta);

		if (!exists("/"))
			throw new N5Exception.N5IOException("No container exists at " + basePath);
	}

	/**
	 * Opens an {@link N5FSReader} at a given base path.
	 *
	 * @param basePath
	 *            N5 base path
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer on the same container will not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5
	 *             version of the container is not compatible with this
	 *             implementation.
	 */
	public N5FSReader(final String basePath, final boolean cacheMeta) throws N5Exception {

		this(basePath, new GsonBuilder(), cacheMeta);
	}

	/**
	 * Opens an {@link N5FSReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param basePath
	 *            N5 base path
	 * @param gsonBuilder
	 *            the gson builder
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5
	 *             version of the container is not compatible with this
	 *             implementation.
	 */
	public N5FSReader(final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

		this(basePath, gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5FSReader} at a given base path.
	 *
	 * @param basePath
	 *            N5 base path
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5
	 *             version of the container is not compatible with this
	 *             implementation.
	 */
	public N5FSReader(final String basePath) throws N5Exception {

		this(basePath, new GsonBuilder(), false);
	}
}
