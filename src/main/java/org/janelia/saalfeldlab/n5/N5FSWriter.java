package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;

/**
 * Filesystem {@link N5Writer} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public class N5FSWriter extends N5KeyValueWriter {

	/**
	 * Opens an {@link N5FSWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath
	 *            n5 base path
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheAttributes
	 *            cache attributes and meta data
	 *            Setting this to true avoidsfrequent reading and parsing of
	 *            JSON encoded attributes andother meta data that requires
	 *            accessing the store. This ismost interesting for high latency
	 *            backends. Changes of cachedattributes and meta data by an
	 *            independent writer on the samecontainer will not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5FSWriter(final String basePath, final GsonBuilder gsonBuilder, final boolean cacheAttributes)
			throws N5Exception {

		super(
				new FileSystemKeyValueRoot(basePath),
				gsonBuilder,
				cacheAttributes);
	}

	/**
	 * Opens an {@link N5FSWriter} at a given base path.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath
	 *            base path
	 * @param cacheAttributes
	 *            attributes and meta data
	 *            Setting this to true avoidsfrequent reading and parsing of
	 *            JSON encoded attributes andother meta data that requires
	 *            accessing the store. This ismost interesting for high latency
	 *            backends. Changes of cachedattributes and meta data by an
	 *            independent writer on the samecontainer will not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5FSWriter(final String basePath, final boolean cacheAttributes) throws N5Exception {

		this(basePath, new GsonBuilder(), cacheAttributes);
	}

	/**
	 * Opens an {@link N5FSWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 * <p>
	 * If the base path does not exist, it will be created.
	 * </p>
	 * <p>
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 * </p>
	 *
	 * @param basePath
	 *            base path
	 * @param gsonBuilder
	 *            gson builder
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5FSWriter(final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

		this(basePath, gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5FSWriter} at a given base path.
	 * <p>
	 * If the base path does not exist, it will be created.
	 * </p>
	 * <p>
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 * </p>
	 *
	 * @param basePath
	 *            n5 base path
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5FSWriter(final String basePath) throws N5Exception {

		this(basePath, new GsonBuilder());
	}
}
