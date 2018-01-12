/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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

/**
 * A simple structured versioned container API for hierarchies
 * of chunked n-dimensional datasets and attributes.
 *
 * {@linkplain https://github.com/axtimwalde/n5}
 *
 * @author Stephan Saalfeld
 */
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
