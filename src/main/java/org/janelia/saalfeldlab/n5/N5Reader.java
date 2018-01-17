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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.scijava.util.VersionUtils;

/**
 * A simple structured container for hierarchies of chunked
 * n-dimensional datasets and attributes.
 *
 * {@linkplain https://github.com/axtimwalde/n5}
 *
 * @author Stephan Saalfeld
 */
public interface N5Reader {

	static public class Version {

		private final int major;
		private final int minor;
		private final int patch;
		private final String suffix;

		public Version(
				final int major,
				final int minor,
				final int patch,
				final String rest) {

			this.major = major;
			this.minor = minor;
			this.patch = patch;
			this.suffix = rest;
		}

		public Version(
				final int major,
				final int minor,
				final int patch) {

			this(major, minor, patch, "");
		}

		/**
		 * Creates a version from a SemVer compatible version string.
		 *
		 * If the version string is null or not a SemVer version, this
		 * version will be "0.0.0"
		 *
		 * @param versionString
		 */
		public Version(final String versionString) {

			System.out.println("version " + versionString);
			boolean isSemVer = false;
			if (versionString != null) {
				final Matcher matcher = Pattern.compile("(\\d+)(\\.(\\d+))?(\\.(\\d+))?(.*)").matcher(versionString);
				isSemVer = matcher.find();
				if (isSemVer) {
					major = Integer.parseInt(matcher.group(1));
					final String minorString = matcher.group(3);
					if (!minorString.equals(""))
						minor = Integer.parseInt(minorString);
					else
						minor = 0;
					final String patchString = matcher.group(5);
					if (!patchString.equals(""))
						patch = Integer.parseInt(patchString);
					else
						patch = 0;
					suffix = matcher.group(6);
				}
				else {
					major = 0;
					minor = 0;
					patch = 0;
					suffix = "";
				}
			}
			else {
				major = 0;
				minor = 0;
				patch = 0;
				suffix = "";
			}
		}

		public final int getMajor() {

			return major;
		}

		public final int getMinor() {

			return minor;
		}

		public final int getPatch() {

			return patch;
		}

		public final String getSuffix() {

			return suffix;
		}

		@Override
		public String toString() {

			final StringBuilder s = new StringBuilder();
			s.append(major);
			s.append(".");
			s.append(minor);
			s.append(".");
			s.append(patch);
			s.append(suffix);

			return s.toString();
		}

		@Override
		public boolean equals(final Object other) {

			if (other instanceof Version) {
				final Version otherVersion = (Version)other;
				return
						(major == otherVersion.major) &
						(minor == otherVersion.minor) &
						(patch == otherVersion.patch) &
						(suffix.equals(otherVersion.suffix));
			}
			else
				return false;
		}

		/**
		 * Returns true if this implementation is compatible with a given version.
		 *
		 * Currently, this means that the version is less than or equal to 1.X.X.
		 *
		 * @param version
		 * @return
		 */
		public boolean isCompatible(final Version version) {

			return version.getMajor() <= major;
		}
	}

	/**
	 * SemVer version of this N5 spec.
	 */
	public static final Version VERSION = new Version(VersionUtils.getVersion(N5Reader.class));

	/**
	 * Version attribute key.
	 */
	public static final String VERSION_KEY = "n5";

	/**
	 * Get the SemVer version of this container as specified in the 'version'
	 * attribute of the root group.
	 *
	 * If no version is specified or the version string does not conform to
	 * the SemVer format, 0.0.0 will be returned.  For incomplete versions,
	 * such as 1.2, the missing elements are filled with 0, i.e. 1.2.0 in this
	 * case.
	 *
	 * @return
	 * @throws IOException
	 */
	public default Version getVersion() throws IOException {

		return new Version(getAttribute("/", VERSION_KEY, String.class));
	}

	/**
	 * Reads an attribute.
	 *
	 * @param pathName group path
	 * @param key
	 * @param clazz attribute class
	 * @return
	 * @throws IOException
	 */
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException;

	/**
	 * Get mandatory dataset attributes.
	 *
	 * @param pathName dataset path
	 * @return dataset attributes or null if either dimensions or dataType are not set
	 * @throws IOException
	 */
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException;

	/**
	 * Reads a {@link DataBlock}.
	 *
	 * @param pathName dataset path
	 * @param datasetAttributes
	 * @param gridPosition
	 * @return
	 * @throws IOException
	 */
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException;

	/**
	 * Test whether a group or dataset exists.
	 *
	 * @param pathName group path
	 * @return
	 */
	public boolean exists(final String pathName);

	/**
	 * Test whether a dataset exists.
	 *
	 * @param pathName dataset path
	 * @return
	 * @throws IOException
	 */
	public default boolean datasetExists(final String pathName) throws IOException {

		return exists(pathName) && getDatasetAttributes(pathName) != null;
	}

	/**
	 * List all groups (including datasets) in a group.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 */
	public String[] list(final String pathName) throws IOException;

	/**
	 * List all attributes and their class of a group.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 */
	public Map<String, Class<?>> listAttributes(final String pathName) throws IOException;
}
