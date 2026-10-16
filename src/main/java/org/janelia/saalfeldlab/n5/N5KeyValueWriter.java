package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;

/**
 * Filesystem {@link N5Writer} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public class N5KeyValueWriter extends N5KeyValueReader implements CachedGsonKeyValueN5Writer {

	/**
	 * Opens an {@link N5KeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 * <p>
	 * If the N5 version of the container is compatible with this
	 * implementation, the N5 version of this container will be set to the
	 * current N5 version of this implementation.
	 *
	 * @param keyValueRoot
	 * 			  the backend key value access to use
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheAttributes
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes, this is most interesting for high
	 *            latency file systems. Changes of attributes by an independent
	 *            writer will not be tracked.
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5KeyValueWriter(
			final KeyValueRoot keyValueRoot,
			final GsonBuilder gsonBuilder,
			final boolean cacheAttributes)
			throws N5Exception {

		super(false, keyValueRoot, gsonBuilder, cacheAttributes, false);

		Version version = null;
		try {
			version = getVersion();
			if (!VERSION.isCompatible(version))
				throw new N5Exception.N5IOException("Incompatible version " + version + " (this is " + VERSION + ").");
		} catch (final NullPointerException e) {}

		if (version == null || version.equals(new Version(0, 0, 0, ""))) {
			createGroup("/");
			setVersion();
		}
	}
}
