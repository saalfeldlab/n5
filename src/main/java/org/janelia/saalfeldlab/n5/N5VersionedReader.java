package org.janelia.saalfeldlab.n5;

import java.io.IOException;

public interface N5VersionedReader extends N5Reader {

	/**
	 * Major SemVer version of this N5 spec.
	 */
	public static final int VERSION_MAJOR = 2;

	/**
	 * Minor SemVer version of this N5 spec.
	 */
	public static final int VERSION_MINOR = 0;

	/**
	 * Patch SemVar version of this N5 spec.
	 */
	public static final int VERSION_PATCH = 0;

	/**
	 * String representation of the SemVer version of this N5 spec.
	 */
	public static final String VERSION = VERSION_MAJOR + "." + VERSION_MINOR + "." + VERSION_PATCH;

	/**
	 * Version attribute key.
	 */
	public static final String VERSION_KEY = "version";

	/**
	 * Check that this container is compatible
	 * with the current N5 specification.
	 *
	 * @return
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	public default void checkVersion() throws IOException, NumberFormatException {

		if (exists("/")) {
			final int[] version = getVersion();
			if (!isCompatible(version[0], version[1], version[2]))
				throw new IOException("Incompatible version " + getVersionString() + " (this is " + VERSION + ").");
		}
	}

	/**
	 * Get the SemVer version [major, minor, patch] of this container
	 * as specified in the 'version' attribute of the root group.
	 *
	 * If no version is specified, 0.0.0 will be returned.
	 * For incomplete versions, such as 1.2, the missing elements are
	 * filled with 0, i.e. 1.2.0 in this case.
	 * If the version does not conform to the "\d+\.\d+\.\d+" format,
	 * a {@link NumberFormatException} is thrown.
	 *
	 * @return
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	public default int[] getVersion() throws IOException, NumberFormatException {

		final String version = getVersionString();
		final int[] semVer = new int[3];
		if (version != null) {
			final String[] components = version.split("\\.");
			if (components.length > 0)
				semVer[0] = Integer.parseInt(components[0]);
			if (components.length > 1)
				semVer[1] = Integer.parseInt(components[1]);
			if (components.length > 2)
				semVer[2] = Integer.parseInt(components[2]);
		}
		return semVer;
	}

	/**
	 * Get the version string of this container
	 * as specified in the 'version' attribute of the root group.
	 *
	 * If no version is specified, {@code null} will be returned.
	 *
	 * @return
	 * @throws IOException
	 */
	public default String getVersionString() throws IOException {

		return getAttribute("/", VERSION_KEY, String.class);
	}

	/**
	 * Returns true if this implementation is compatible with a given version.
	 *
	 * Currently, this means that the version is less than or equal to 1.0.0.
	 *
	 * @param major
	 * @param minor
	 * @param patch
	 * @return
	 */
	public static boolean isCompatible(final int major, final int minor, final int patch) {

		if (major > VERSION_MAJOR)
			return false;
		if (minor > VERSION_MINOR)
			return false;
		if (patch > VERSION_PATCH)
			return false;
		return true;
	}
}
